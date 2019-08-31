/*
 * Copyright 2017, 2018 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.etcd.client.config;

import static com.ibm.etcd.client.KeyUtils.bs;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;

/**
 * See etcd-json-schema.md for the json schema for etcd cluster config
 * 
 */
public class EtcdClusterConfig {

    private static final Logger logger = LoggerFactory.getLogger(EtcdClusterConfig.class);

    public static final int DEFAULT_MAX_MSG_SIZE = 256 * 1024 * 1024; // 256 MiB

    //TODO later make this more configurable/narrow; only really needed for getting large ranges
    protected final int maxMessageSize = Integer.getInteger("etcd-java.maxMessageSize",
            DEFAULT_MAX_MSG_SIZE);

    public enum TlsMode { TLS, PLAINTEXT, AUTO }

    Set<String> endpoints;
    TlsMode tlsMode;
    ByteString user, password;
    ByteString rootPrefix; // a.k.a namespace
    String composeDeployment;
    ByteSource certificate;

    protected EtcdClusterConfig() {}

    public ByteString getRootPrefix() {
        return rootPrefix;
    }

    public Set<String> getEndpoints() {
        return endpoints;
    }

    public EtcdClient getClient() throws IOException, CertificateException {
        return getClient(this);
    }

    private EtcdClient newClient() throws IOException, CertificateException {
        List<String> endpointList = new ArrayList<>(endpoints);
        EtcdClient.Builder builder = EtcdClient.forEndpoints(endpointList)
                .withCredentials(user, password).withImmediateAuth()
                .withMaxInboundMessageSize(maxMessageSize);
        TlsMode ssl = tlsMode;
        if (ssl == TlsMode.AUTO || ssl == null) {
            String ep = endpointList.get(0);
            ssl = ep.startsWith("https://")
                    || (!ep.startsWith("http://") && certificate != null)
                    ? TlsMode.TLS : TlsMode.PLAINTEXT;
        }
        if (ssl == TlsMode.PLAINTEXT) {
            builder.withPlainText();
        } else if (composeDeployment != null) {
            builder.withTrustManager(new ComposeTrustManagerFactory(composeDeployment,
                    composeDeployment, certificate));
        } else if (certificate != null) {
            builder.withCaCert(certificate);
        }
        if (isShutdown) {
            throw new IllegalStateException("shutdown");
        }
        return builder.build();
    }

    // mainly for testing
    public static EtcdClusterConfig newSimpleConfig(String endpoints, String rootPrefix) {
        EtcdClusterConfig config = new EtcdClusterConfig();
        config.endpoints = Sets.newHashSet(endpoints.split(","));
        config.rootPrefix = bs(rootPrefix);
        return config;
    }

    public static EtcdClusterConfig fromProperties(ByteSource source) throws IOException {
        Properties props = new Properties();
        try (InputStream in = source.openStream()) {
            props.load(in);
        }
        String epString = props.getProperty("endpoints");
        if (epString == null) {
            throw new IOException("etcd config must contain endpoints property");
        }
        EtcdClusterConfig config = new EtcdClusterConfig();
        config.endpoints = Sets.newHashSet(epString.split(","));
        config.user = bs(props.getProperty("username"));
        config.password = bs(props.getProperty("password"));
        config.composeDeployment = props.getProperty("compose_deployment");
        config.rootPrefix = bs(props.getProperty("root_prefix")); // a.k.a namespace
        String tlsMode = props.getProperty("tls_mode");
        if (tlsMode != null) {
            config.tlsMode = TlsMode.valueOf(tlsMode);
        }
        String certPath = props.getProperty("certificate_file");
        if (certPath != null) {
            File certFile = new File(certPath);
            if (!certFile.exists()) {
                throw new IOException("cant find certificate file: " + certPath);
            }
            config.certificate = Files.asByteSource(certFile);
        }
        return config;
    }

    public static EtcdClusterConfig fromJson(ByteSource source, File dir) throws IOException {
        JsonConfig jsonConfig;
        try (InputStream in = source.openStream()) {
            jsonConfig = deserializeJson(in);
        }
        if (jsonConfig.endpoints == null || jsonConfig.endpoints.trim().isEmpty()) {
            throw new IOException("etcd config must contain endpoints property");
        }
        EtcdClusterConfig config = new EtcdClusterConfig();
        config.endpoints = Sets.newHashSet(jsonConfig.endpoints.split(","));
        config.user = bs(jsonConfig.user);
        config.password = bs(jsonConfig.password);
        config.composeDeployment = jsonConfig.composeDeployment;
        config.rootPrefix = bs(jsonConfig.rootPrefix);
        if (jsonConfig.certificateFile != null) {
            File certFile = new File(jsonConfig.certificateFile);
            if (dir != null && !certFile.exists()) {
                // try same dir as the config file
                certFile = new File(dir, jsonConfig.certificateFile);
            }
            if (certFile.exists()) {
                config.certificate = Files.asByteSource(certFile);
            } else {
                // will fall back to embedded if present
                logger.warn("Can't find certificate file: " + jsonConfig.certificateFile);
            }
        }
        if (jsonConfig.certificate != null) {
            if (config.certificate != null) {
                logger.warn("Ignoring json-embedded cert because file was also provided");
            } else {
                config.certificate = ByteSource.wrap(jsonConfig.certificate.getBytes(UTF_8));
            }
        }
        return config;
    }

    public static EtcdClusterConfig fromJson(ByteSource source) throws IOException {
        return fromJson(source, null);
    }

    public static EtcdClusterConfig fromJsonFile(String file) throws IOException {
        File f = new File(file);
        return fromJson(Files.asByteSource(f), f.getParentFile());
    }

    protected static final String ADDR_STR = "(?:https?://)?(?:[a-zA-Z0-9\\-.]+)(?::\\d+)?";
    protected static final Pattern SIMPLE_PATT = Pattern.compile(String
            .format("((?:%s)(?:,%s)*)(?:;rootPrefix=(.+))?", ADDR_STR, ADDR_STR));

    /**
     * @param fileOrSimpleString path to json config file <b>or</b> simple config of the form
     *     "endpoint1,endpoint2,...;rootPrefix=&lt;prefix&gt;", where ;rootPrefix=&lt;prefix&gt;
     *     is optional.
     * @throws IOException
     */
    public static EtcdClusterConfig fromJsonFileOrSimple(String fileOrSimpleString) throws IOException {
        File f = new File(fileOrSimpleString);
        if (f.exists()) {
            return fromJson(Files.asByteSource(f), f.getParentFile());
        }
        Matcher m = SIMPLE_PATT.matcher(fileOrSimpleString);
        if (m.matches()) {
            return EtcdClusterConfig.newSimpleConfig(m.group(1), m.group(2));
        }
        throw new FileNotFoundException("etcd config json file not found: " + f);
    }

    private static final Cache<CacheKey,EtcdClient> clientCache = CacheBuilder.newBuilder().weakValues()
            .<CacheKey,EtcdClient>removalListener(rn -> rn.getValue().close()).build();

    public static EtcdClient getClient(EtcdClusterConfig config) throws IOException, CertificateException {
        try {
            return clientCache.get(new CacheKey(config), config::newClient);
        } catch (ExecutionException ee) {
            Throwables.throwIfInstanceOf(ee.getCause(), IOException.class);
            Throwables.throwIfInstanceOf(ee.getCause(), CertificateException.class);
            Throwables.throwIfUnchecked(ee.getCause());
            throw new RuntimeException(ee.getCause());
        }
    }

    private static volatile boolean isShutdown = false;

    /**
     * Should generally only be called during JVM shutdown
     */
    public static void shutdownAll() {
        isShutdown = true;
        clientCache.invalidateAll();
    }

    static class CacheKey {
        private final EtcdClusterConfig config;

        CacheKey(EtcdClusterConfig config) {
            this.config = Preconditions.checkNotNull(config);
        }

        // NOTE: rootPrefix is currently intentionally excluded
        // since it's not used to build the client
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CacheKey)) {
                return false;
            }
            EtcdClusterConfig other = ((CacheKey) obj).config;
            return Objects.equals(config.endpoints, other.endpoints)
                    && Objects.equals(config.composeDeployment, other.composeDeployment)
                    && Objects.equals(config.user, other.user)
                    && Objects.equals(config.tlsMode, other.tlsMode);
        }
        @Override
        public int hashCode() {
            return Objects.hash(config.endpoints, config.user,
                    config.composeDeployment, config.tlsMode);
        }
    }

    // ----  json deserialization

    private static final Gson gson = new Gson();

    private static JsonConfig deserializeJson(InputStream in) {
        return gson.fromJson(new InputStreamReader(in, StandardCharsets.UTF_8), JsonConfig.class);
    }

    static class JsonConfig {
        @SerializedName("endpoints")
        String endpoints;
        @SerializedName("userid")
        String user;
        @SerializedName("password")
        String password;
        @SerializedName("root_prefix") // a.k.a namespace
        String rootPrefix;
        @SerializedName("compose_deployment")
        String composeDeployment;
        @SerializedName("certificate")
        String certificate;
        @SerializedName("certificate_file")
        String certificateFile;
    }
}
