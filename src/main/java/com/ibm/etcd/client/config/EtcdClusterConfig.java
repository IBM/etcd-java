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
    @Deprecated
    String composeDeployment;
    ByteSource certificate;
    String overrideAuthority;
    // either both or neither of these must be set
    ByteSource clientKey;
    ByteSource clientCertificate;

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
        EtcdClient.Internal.makeRefCounted(builder);
        if (overrideAuthority != null) {
            builder.overrideAuthority(overrideAuthority);
        }
        TlsMode ssl = tlsMode;
        if (ssl == TlsMode.AUTO || ssl == null) {
            String ep = endpointList.get(0);
            ssl = ep.startsWith("https://")
                    || (!ep.startsWith("http://")
                            && (certificate != null && clientCertificate != null))
                    ? TlsMode.TLS : TlsMode.PLAINTEXT;
        }
        if (ssl == TlsMode.PLAINTEXT) {
            builder.withPlainText();
        } else {
            if (composeDeployment != null) {
                builder.withTrustManager(new ComposeTrustManagerFactory(composeDeployment,
                        composeDeployment, certificate));
            } else if (certificate != null) {
                builder.withCaCert(certificate);
            }
            if (clientCertificate != null && clientKey != null) {
                try (InputStream keyStream = clientKey.openStream();
                     InputStream certStream = clientCertificate.openStream()) {
                    builder.withTlsConfig(b -> b.keyManager(certStream, keyStream));
                }
            }
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
        if (tlsMode != null) try {
            config.tlsMode = TlsMode.valueOf(tlsMode);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Invalid value "
                    + tlsMode + " for etcd tls_mode config property");
        }
        config.clientKey = certFromProperties("client_key", props);
        config.clientCertificate = certFromProperties("client_certificate", props);
        config.certificate = certFromProperties("certificate_file", props);
        config.overrideAuthority = props.getProperty("override_authority");
        return validateConfig(config);
    }

    private static ByteSource certFromProperties(String certFilePropName,
            Properties props) throws IOException {
        String certPath = props.getProperty(certFilePropName);
        if (certPath == null) {
            return null;
        }
        File certFile = new File(certPath);
        if (certFile.exists()) {
            return Files.asByteSource(certFile);
        }
        throw new IOException("cant find certificate file: " + certPath);
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
        if (jsonConfig.tlsMode != null) try {
            config.tlsMode = TlsMode.valueOf(jsonConfig.tlsMode);
        } catch (IllegalArgumentException iae) {
            throw new IOException("Invalid value "
                    + jsonConfig.tlsMode + " for etcd tls_mode config field");
        }
        config.clientKey = certFromJson(jsonConfig.clientKeyFile, jsonConfig.clientKey, dir);
        config.clientCertificate = certFromJson(
                jsonConfig.clientCertificateFile, jsonConfig.clientCertificate, dir);
        config.certificate = certFromJson(jsonConfig.certificateFile, jsonConfig.certificate, dir);
        config.overrideAuthority = jsonConfig.overrideAuthority;
        return validateConfig(config);
    }

    private static ByteSource certFromJson(String certFileName, String literalCert,
            File dir) throws IOException {
        if (certFileName != null) {
            File certFile = new File(certFileName);
            if (dir != null && !certFile.exists()) {
                // try same dir as the config file
                certFile = new File(dir, certFileName);
            }
            if (certFile.exists()) {
                if (literalCert != null) {
                    logger.warn("Ignoring json-embedded cert because file "
                            + certFileName + " was also provided");
                }
                return Files.asByteSource(certFile);
            } else if (literalCert != null) {
                // will fall back to embedded if present
                logger.warn("Can't find certificate file: " + certFileName);
            } else {
                throw new IOException("Can't find certificate file: " + certFileName);
            }
        }
        return literalCert != null ? ByteSource.wrap(literalCert.getBytes(UTF_8)) : null;
    }

    private static EtcdClusterConfig validateConfig(EtcdClusterConfig config) throws IOException {
        if (config.composeDeployment != null) {
            logger.warn("compose_deployment config param is deprecated," +
                    " use override_authority to set a specific name for TLS SNI");
        }
        if ((config.clientKey == null) != (config.clientCertificate == null)) {
            throw new IOException("Must specify either both or neither of TLS client_key "
                    + "and client_certificate attributes");
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

    private static final Cache<CacheKey, EtcdClient> clientCache = CacheBuilder.newBuilder().weakValues()
            .<CacheKey, EtcdClient>removalListener(rn -> EtcdClient.Internal.cleanup(rn.getValue())).build();

    public static EtcdClient getClient(EtcdClusterConfig config) throws IOException, CertificateException {
        try {
            // Share equivalent ref-counted client from cache if possible
            final CacheKey key = new CacheKey(config);
            while (true) {
                EtcdClient client = clientCache.getIfPresent(key);
                if (client == null) {
                    EtcdClient[] maybeNew = new EtcdClient[1];
                    client  = clientCache.get(key, () -> maybeNew[0] = config.newClient());
                    // if client != maybeNew[0] then we got a pre-existing client
                    if (client != maybeNew[0] && !EtcdClient.Internal.retain(client)) {
                        clientCache.invalidate(key);
                        continue;
                    }
                }
                return client;
            }
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
                    && Objects.equals(config.tlsMode, other.tlsMode)
                    && (config.certificate == null) == (other.certificate == null)
                    && (config.clientKey == null) == (other.clientKey == null)
                    && (config.clientCertificate == null) == (other.clientCertificate == null);
        }
        @Override
        public int hashCode() {
            return Objects.hash(config.endpoints, config.user,
                    config.composeDeployment, config.tlsMode, config.certificate == null,
                    config.clientKey == null, config.clientCertificate == null);
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
        @Deprecated
        @SerializedName("compose_deployment")
        String composeDeployment;
        @SerializedName("tls_mode")
        String tlsMode;
        @SerializedName("certificate")
        String certificate;
        @SerializedName("certificate_file")
        String certificateFile;
        @SerializedName("override_authority")
        String overrideAuthority;
        @SerializedName("client_key")
        String clientKey;
        @SerializedName("client_key_file")
        String clientKeyFile;
        @SerializedName("client_certificate")
        String clientCertificate;
        @SerializedName("client_certificate_file")
        String clientCertificateFile;
    }
}
