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

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteSource;

import io.netty.handler.ssl.util.SimpleTrustManagerFactory;
import io.netty.util.internal.EmptyArrays;

/**
 * Custom trust manager to use with multi-endpoint IBM Compose etcd deployments.
 * Works around grpc-java TLS issues.
 * 
 */
public class ComposeTrustManagerFactory extends SimpleTrustManagerFactory {

    private static final Logger logger = LoggerFactory.getLogger(ComposeTrustManagerFactory.class);

    private final TrustManager tm;
    
    private final X509TrustManager defaultTm;
    
    public ComposeTrustManagerFactory(String deploymentName)
            throws CertificateException, IOException  {
        this(deploymentName, deploymentName, null);
    }

    public ComposeTrustManagerFactory(String name, final String deploymentName,
            ByteSource certSource) throws CertificateException, IOException {
        super(name);

        final X509Certificate cert;
        if(certSource == null) cert = null;
        else {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            try(InputStream in = certSource.openStream()) {
                cert = (X509Certificate) certFactory.generateCertificate(in);
            }
        }
        
        this.defaultTm = getDefaultTrustManager(cert);

        this.tm = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] chain, String str) {
                logger.info("Accepting a client certificate: " + chain[0].getSubjectDN() );
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String str) throws CertificateException {
                final X509Certificate serverCert = chain[0];
                boolean ok = false;
                if(defaultTm != null) try {
                    // first try regular trustmanager
                    defaultTm.checkServerTrusted(chain, str);
                    ok = true;
                } catch(CertificateException ce) {}
                if( !ok && !serverCert.getSubjectDN().getName().equalsIgnoreCase(deploymentName)
                        && !serverCert.getSubjectDN().getName().equalsIgnoreCase("CN=" + deploymentName) ) {
                    throw new CertificateException("Certificate with unknown deployment: "
                            + serverCert.getSubjectDN().getName());
                }
                if( cert != null ) {
                    if ( !serverCert.getIssuerDN().equals(cert.getIssuerDN()) ) {
                        throw new CertificateException("Certificate Issuers do not match: "
                                + serverCert.getIssuerDN());

                    }
                    // See if certs are the same, if so we are good.
                    if( !serverCert.equals(cert) ) try {
                        // Not your CA's. Check if it has been signed by your CA
                        serverCert.verify(cert.getPublicKey());
                    } catch ( Exception exc ) {
                        throw new CertificateException("Certificate not trusted", exc);
                    }
                }
                
                // this will throw if certificate's date is invalid (expired or not yet valid)
                serverCert.checkValidity();
                
                if(logger.isDebugEnabled()) {
                    logger.debug("Accepting a server certificate: "
                            + serverCert.getSubjectDN().getName());
                }
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return cert != null ? new X509Certificate[] { cert } : EmptyArrays.EMPTY_X509_CERTIFICATES;
            }
        };
    }
    
    private static X509TrustManager getDefaultTrustManager(X509Certificate cert) {
        try {
            TrustManagerFactory defaultTmf = TrustManagerFactory
                    .getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore ks = null;
            if(cert != null) {
                ks = KeyStore.getInstance(KeyStore.getDefaultType());
                ks.setCertificateEntry("compose-provided", cert);
            }
            defaultTmf.init(ks);
            if(defaultTmf != null) for(TrustManager m : defaultTmf.getTrustManagers()) {
                if(m instanceof X509TrustManager) return (X509TrustManager) m;
            }
        } catch (NoSuchAlgorithmException | KeyStoreException e) {
            logger.warn("Failed to look up default TrustManager", e);
        }
        return null;
    }
    
    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[] { tm };
    }

    @Override
    protected void engineInit(KeyStore keyStore) throws Exception {}

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws Exception {}

}
