/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.geoip;

import com.sun.net.httpserver.HttpServer;

import org.elasticsearch.common.hash.MessageDigests;
import org.junit.rules.ExternalResource;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;

/**
 * This fixture is used to simulate a maxmind-provided server for downloading maxmind geoip database files from the
 * EnterpriseGeoIpDownloader. It can be used by integration tests so that they don't actually hit maxmind servers.
 */
public class EnterpriseGeoIpHttpFixture extends ExternalResource {

    private final List<String> maxmindDatabaseTypes;
    private HttpServer server;

    /*
     * The values in databaseTypes must be in DatabaseConfiguration.MAXMIND_NAMES.
     */
    public EnterpriseGeoIpHttpFixture(List<String> maxmindDatabaseTypes) {
        this.maxmindDatabaseTypes = List.copyOf(maxmindDatabaseTypes);
    }

    public String getAddress() {
        return "http://" + server.getAddress().getHostString() + ":" + server.getAddress().getPort() + "/";
    }

    @Override
    protected void before() throws Throwable {
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);

        // for expediency reasons, it is handy to have this test fixture be able to serve the dual purpose of actually stubbing
        // out the download protocol for downloading files from maxmind (see the looped context creation after this stanza), as
        // we as to serve an empty response for the geoip.elastic.co service here
        this.server.createContext("/", exchange -> {
            String response = "[]"; // an empty json array
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes(StandardCharsets.UTF_8));
            }
        });

        // register the file types for the download fixture
        for (String databaseType : maxmindDatabaseTypes) {
            createContextForMaxmindDatabase(databaseType);
        }

        server.start();
    }

    private void createContextForMaxmindDatabase(String databaseType) {
        this.server.createContext("/" + databaseType + "/download", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            if (exchange.getRequestURI().toString().contains("sha256")) {
                MessageDigest sha256 = MessageDigests.sha256();
                try (InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/" + databaseType + ".tgz")) {
                    sha256.update(inputStream.readAllBytes());
                }
                exchange.getResponseBody()
                    .write(
                        (MessageDigests.toHexString(sha256.digest()) + "  " + databaseType + "_20240709.tar.gz").getBytes(
                            StandardCharsets.UTF_8
                        )
                    );
            } else {
                try (
                    OutputStream outputStream = exchange.getResponseBody();
                    InputStream inputStream = GeoIpHttpFixture.class.getResourceAsStream("/geoip-fixture/" + databaseType + ".tgz")
                ) {
                    inputStream.transferTo(outputStream);
                }
            }
            exchange.getResponseBody().close();
        });
    }

    @Override
    protected void after() {
        server.stop(0);
    }
}
