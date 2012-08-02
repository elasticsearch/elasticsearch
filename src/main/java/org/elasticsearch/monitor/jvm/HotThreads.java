/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.unit.TimeValue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 */
public class HotThreads {

    private int busiestThreads = 3;
    private TimeValue interval = new TimeValue(500, TimeUnit.MILLISECONDS);
    private TimeValue threadElementsSnapshotDelay = new TimeValue(10);
    private int threadElementsSnapshotCount = 10;
    private String type = "cpu";

    public HotThreads interval(TimeValue interval) {
        this.interval = interval;
        return this;
    }

    public HotThreads busiestThreads(int busiestThreads) {
        this.busiestThreads = busiestThreads;
        return this;
    }

    public HotThreads threadElementsSnapshotDelay(TimeValue threadElementsSnapshotDelay) {
        this.threadElementsSnapshotDelay = threadElementsSnapshotDelay;
        return this;
    }

    public HotThreads threadElementsSnapshotCount(int threadElementsSnapshotCount) {
        this.threadElementsSnapshotCount = threadElementsSnapshotCount;
        return this;
    }

    public HotThreads type(String type) {
        if ("cpu".equals(type) || "wait".equals(type) || "block".equals(type)) {
            this.type = type;
        } else {
            throw new ElasticSearchIllegalArgumentException("type not supported [" + type + "]");
        }
        return this;
    }

    public String detect() throws Exception {
        StringBuilder sb = new StringBuilder();
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        if (threadBean.isThreadCpuTimeSupported()) {
            if (!threadBean.isThreadCpuTimeEnabled()) threadBean.setThreadCpuTimeEnabled(true);
        } else {
            throw new IllegalStateException("MBean doesn't support thread CPU Time");
        }
        Map<Long, MyThreadInfo> threadInfos = new HashMap<Long, MyThreadInfo>();
        for (long threadId : threadBean.getAllThreadIds()) {
            // ignore our own thread...
            if (Thread.currentThread().getId() == threadId) {
                continue;
            }
            long cpu = threadBean.getThreadCpuTime(threadId);
            ThreadInfo info = threadBean.getThreadInfo(threadId, 0);
            threadInfos.put(threadId, new MyThreadInfo(cpu, info));
        }
        Thread.sleep(interval.millis());
        for (long threadId : threadBean.getAllThreadIds()) {
            // ignore our own thread...
            if (Thread.currentThread().getId() == threadId) {
                continue;
            }
            long cpu = threadBean.getThreadCpuTime(threadId);
            ThreadInfo info = threadBean.getThreadInfo(threadId, 0);
            MyThreadInfo data = threadInfos.get(threadId);
            if (data != null) {
                data.setDelta(cpu, info);
            }
        }
        // sort by delta CPU time on thread.
        List<MyThreadInfo> hotties = new ArrayList<MyThreadInfo>(threadInfos.values());
        // skip that for now
        Collections.sort(hotties, new Comparator<MyThreadInfo>() {
            public int compare(MyThreadInfo o1, MyThreadInfo o2) {
                if ("cpu".equals(type)) {
                    return (int) (o2.cpuTime - o1.cpuTime);
                } else if ("wait".equals(type)) {
                    return (int) (o2.waitedTime - o1.waitedTime);
                } else if ("block".equals(type)) {
                    return (int) (o2.blockedTime - o1.blockedTime);
                }
                throw new IllegalArgumentException();
            }
        });
//        for(MyThreadInfo inf : hotties) {
//            if(inf.deltaDone) {
//                System.out.format("%5.2f %d/%d %d/%d %s%n",
//                    inf.cpuTime/1E7,
//                    inf.blockedCount,
//                    inf.blockedTime,
//                    inf.waitedCount,
//                    inf.waitedtime,
//                    inf.info.getThreadName()
//                    );
//            }
//        }
        // analyse N stack traces for M busiest threads
        long[] ids = new long[busiestThreads];
        for (int i = 0; i < busiestThreads; i++) {
            MyThreadInfo info = hotties.get(i);
            ids[i] = info.info.getThreadId();
        }
        ThreadInfo[][] allInfos = new ThreadInfo[threadElementsSnapshotCount][];
        for (int j = 0; j < threadElementsSnapshotCount; j++) {
            allInfos[j] = threadBean.getThreadInfo(ids, Integer.MAX_VALUE);
            Thread.sleep(threadElementsSnapshotDelay.millis());
        }
        for (int t = 0; t < busiestThreads; t++) {
            double value = -1;
            if ("cpu".equals(type)) {
                value = hotties.get(t).cpuTime / 1E7;
            } else if ("wait".equals(type)) {
                value = hotties.get(t).waitedTime / 1E7;
            } else if ("block".equals(type)) {
                value = hotties.get(t).blockedTime / 1E7;
            }
            sb.append(String.format("%n%4.1f%% %s usage by thread '%s'%n", value, type, allInfos[0][t].getThreadName()));
            // for each snapshot (2nd array index) find later snapshot for same thread with max number of
            // identical StackTraceElements (starting from end of each)
            boolean[] done = new boolean[threadElementsSnapshotCount];
            for (int i = 0; i < threadElementsSnapshotCount; i++) {
                if (done[i]) continue;
                int maxSim = 1;
                boolean[] similars = new boolean[threadElementsSnapshotCount];
                for (int j = i + 1; j < threadElementsSnapshotCount; j++) {
                    if (done[j]) continue;
                    int similarity = similarity(allInfos[i][t], allInfos[j][t]);
                    if (similarity > maxSim) {
                        maxSim = similarity;
                        similars = new boolean[threadElementsSnapshotCount];
                    }
                    if (similarity == maxSim) similars[j] = true;
                }
                // print out trace maxSim levels of i, and mark similar ones as done
                int count = 1;
                for (int j = i + 1; j < threadElementsSnapshotCount; j++) {
                    if (similars[j]) {
                        done[j] = true;
                        count++;
                    }
                }
                StackTraceElement[] show = allInfos[i][t].getStackTrace();
                if (count == 1) {
                    sb.append(String.format("  unique snapshot%n"));
                    for (int l = 0; l < show.length; l++) {
                        sb.append(String.format("    %s%n", show[l]));
                    }
                } else {
                    sb.append(String.format("  %d/%d snapshots sharing following %d elements%n", count, threadElementsSnapshotCount, maxSim));
                    for (int l = show.length - maxSim; l < show.length; l++) {
                        sb.append(String.format("    %s%n", show[l]));
                    }
                }
            }
        }
        return sb.toString();
    }

    private int similarity(ThreadInfo threadInfo, ThreadInfo threadInfo0) {
        StackTraceElement[] s1 = threadInfo.getStackTrace();
        StackTraceElement[] s2 = threadInfo0.getStackTrace();
        int i = s1.length - 1;
        int j = s2.length - 1;
        int rslt = 0;
        while (i >= 0 && j >= 0 && s1[i].equals(s2[j])) {
            rslt++;
            i--;
            j--;
        }
        return rslt;
    }


    class MyThreadInfo {
        long cpuTime;
        long blockedCount;
        long blockedTime;
        long waitedCount;
        long waitedTime;
        boolean deltaDone;
        ThreadInfo info;

        MyThreadInfo(long cpuTime, ThreadInfo info) {
            blockedCount = info.getBlockedCount();
            blockedTime = info.getBlockedTime();
            waitedCount = info.getWaitedCount();
            waitedTime = info.getWaitedTime();
            this.cpuTime = cpuTime;
        }

        void setDelta(long cpuTime, ThreadInfo info) {
            if (deltaDone) throw new IllegalStateException("setDelta already called once");
            blockedCount = info.getBlockedCount() - blockedCount;
            blockedTime = info.getBlockedTime() - blockedTime;
            waitedCount = info.getWaitedCount() - waitedCount;
            waitedTime = info.getWaitedTime() - waitedTime;
            this.cpuTime = cpuTime - this.cpuTime;
            deltaDone = true;
            this.info = info;
        }
    }
}