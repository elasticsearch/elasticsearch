/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractor;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class AnalyticsProcessManager {

    private static final Logger LOGGER = LogManager.getLogger(AnalyticsProcessManager.class);

    private final Client client;
    private final ThreadPool threadPool;
    private final AnalyticsProcessFactory processFactory;
    private final ConcurrentMap<Long, ProcessContext> processContextByAllocation = new ConcurrentHashMap<>();

    public AnalyticsProcessManager(Client client, Environment environment, ThreadPool threadPool,
                                   AnalyticsProcessFactory analyticsProcessFactory) {
        this.client = Objects.requireNonNull(client);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.processFactory = Objects.requireNonNull(analyticsProcessFactory);
    }

    public void runJob(long taskAllocationId, DataFrameAnalyticsConfig config, DataFrameDataExtractorFactory dataExtractorFactory,
                       Consumer<Exception> finishHandler) {
        threadPool.generic().execute(() -> {
            DataFrameDataExtractor dataExtractor = dataExtractorFactory.newExtractor(false);
            AnalyticsProcess process = createProcess(config.getId(), createProcessConfig(config, dataExtractor));
            processContextByAllocation.putIfAbsent(taskAllocationId, new ProcessContext());
            ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
            DataFrameRowsJoiner dataFrameRowsJoiner = new DataFrameRowsJoiner(config.getId(), client,
                dataExtractorFactory.newExtractor(true));
            AnalyticsResultProcessor resultProcessor = new AnalyticsResultProcessor(processContextByAllocation.get(taskAllocationId),
                dataFrameRowsJoiner);
            executorService.execute(() -> resultProcessor.process(process));
            executorService.execute(
                () -> processData(taskAllocationId, config, dataExtractor, process, resultProcessor, finishHandler));
        });
    }

    private void processData(long taskAllocationId, DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor,
                             AnalyticsProcess process, AnalyticsResultProcessor resultProcessor, Consumer<Exception> finishHandler) {
        try {
            writeHeaderRecord(dataExtractor, process);
            writeDataRows(dataExtractor, process);
            process.writeEndOfDataMessage();
            process.flushStream();

            LOGGER.info("[{}] Waiting for result processor to complete", config.getId());
            resultProcessor.awaitForCompletion();
            refreshDest(config);
            LOGGER.info("[{}] Result processor has completed", config.getId());
        } catch (IOException e) {
            LOGGER.error(new ParameterizedMessage("[{}] Error writing data to the process", config.getId()), e);
            // TODO Handle this failure by setting the task state to FAILED
        } finally {
            LOGGER.info("[{}] Closing process", config.getId());
            try {
                process.close();
                LOGGER.info("[{}] Closed process", config.getId());

                // This results in marking the persistent task as complete
                finishHandler.accept(null);
            } catch (IOException e) {
                LOGGER.error("[{}] Error closing data frame analyzer process", config.getId());
                finishHandler.accept(e);
            }
            processContextByAllocation.remove(taskAllocationId);
        }
    }

    private void writeDataRows(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
        // The extra fields are for the doc hash and the control field (should be an empty string)
        String[] record = new String[dataExtractor.getFieldNames().size() + 2];
        // The value of the control field should be an empty string for data frame rows
        record[record.length - 1] = "";

        while (dataExtractor.hasNext()) {
            Optional<List<DataFrameDataExtractor.Row>> rows = dataExtractor.next();
            if (rows.isPresent()) {
                for (DataFrameDataExtractor.Row row : rows.get()) {
                    if (row.shouldSkip() == false) {
                        String[] rowValues = row.getValues();
                        System.arraycopy(rowValues, 0, record, 0, rowValues.length);
                        record[record.length - 2] = String.valueOf(row.getChecksum());
                        process.writeRecord(record);
                    }
                }
            }
        }
    }

    private void writeHeaderRecord(DataFrameDataExtractor dataExtractor, AnalyticsProcess process) throws IOException {
        List<String> fieldNames = dataExtractor.getFieldNames();

        // We add 2 extra fields, both named dot:
        //   - the document hash
        //   - the control message
        String[] headerRecord = new String[fieldNames.size() + 2];
        for (int i = 0; i < fieldNames.size(); i++) {
            headerRecord[i] = fieldNames.get(i);
        }

        headerRecord[headerRecord.length - 2] = ".";
        headerRecord[headerRecord.length - 1] = ".";
        process.writeRecord(headerRecord);
    }

    private AnalyticsProcess createProcess(String jobId, AnalyticsProcessConfig analyticsProcessConfig) {
        ExecutorService executorService = threadPool.executor(MachineLearning.JOB_COMMS_THREAD_POOL_NAME);
        AnalyticsProcess process = processFactory.createAnalyticsProcess(jobId, analyticsProcessConfig, executorService);
        if (process.isProcessAlive() == false) {
            throw ExceptionsHelper.serverError("Failed to start data frame analytics process");
        }
        return process;
    }

    private AnalyticsProcessConfig createProcessConfig(DataFrameAnalyticsConfig config, DataFrameDataExtractor dataExtractor) {
        DataFrameDataExtractor.DataSummary dataSummary = dataExtractor.collectDataSummary();
        AnalyticsProcessConfig processConfig = new AnalyticsProcessConfig(dataSummary.rows, dataSummary.cols,
                config.getModelMemoryLimit(), 1, config.getDest().getResultsField(), config.getAnalysis());
        return processConfig;
    }

    @Nullable
    public Integer getProgressPercent(long allocationId) {
        ProcessContext processContext = processContextByAllocation.get(allocationId);
        return processContext == null ? null : processContext.progressPercent.get();
    }

    private void refreshDest(DataFrameAnalyticsConfig config) {
        ClientHelper.executeWithHeaders(config.getHeaders(), ClientHelper.ML_ORIGIN, client,
            () -> client.execute(RefreshAction.INSTANCE, new RefreshRequest(config.getDest().getIndex())).actionGet());
    }

    static class ProcessContext {

        private final AtomicInteger progressPercent = new AtomicInteger(0);

        void setProgressPercent(int progressPercent) {
            this.progressPercent.set(progressPercent);
        }
    }
}
