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
package com.ibm.etcd.client;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.internal.DnsNameResolverProvider;

/**
 * NameResolver for static list of URIs. Delegates individual address resolution
 * to gRPC DnsNameResolver.
 */
class StaticEtcdNameResolverFactory extends NameResolver.Factory {

    public static final String ETCD = "etcd";
    
    protected static final NameResolver.Factory DNS_PROVIDER = new DnsNameResolverProvider();
    
    // (not intended to be strict hostname validation here)
    protected static final Pattern ADDR_PATT =
            Pattern.compile("(?:https?://)?([a-zA-Z0-9\\-.]+)(?::(\\d+))?");
    
    static class SubResolver {
        final NameResolver resolver;
        List<EquivalentAddressGroup> eagList = Collections.emptyList();
        
        public SubResolver(URI uri) {
            this.resolver = DNS_PROVIDER.newNameResolver(uri, Attributes.EMPTY);
        }
    }
    
    private final SubResolver[] resolvers;
    
    public StaticEtcdNameResolverFactory(List<String> endpoints) {
        if(endpoints == null || endpoints.isEmpty()) throw new IllegalArgumentException("endpoints");
        int count = endpoints.size();
        resolvers = new SubResolver[count];
        for(int i=0;i< count;i++) {
            String endpoint = endpoints.get(i).trim();
            Matcher m = ADDR_PATT.matcher(endpoint);
            if(!m.matches()) throw new IllegalArgumentException("invalid endpoint: "+endpoint);
            String portStr = m.group(2);
            if(portStr == null) portStr = String.valueOf(EtcdClient.DEFAULT_PORT);
            resolvers[i] = new SubResolver(URI.create("dns:///"+m.group(1)+":"+portStr));
        }
        if(count > 1) Collections.shuffle(Arrays.asList(resolvers));
    }
    
    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        if(!ETCD.equals(targetUri.getScheme())) return null;
        if(resolvers.length == 1) {
            return resolvers[0].resolver;
        }
        return new NameResolver() {
            int currentCount = 0;
            @Override
            public void start(Listener listener) {
                for(SubResolver sr : resolvers) sr.resolver.start(new Listener() {
                    @Override
                    public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
                        synchronized(resolvers) {
                            sr.eagList = servers;
                            List<EquivalentAddressGroup> newList = Stream.of(resolvers).flatMap(r
                                    -> r.eagList.stream()).collect(Collectors.toList());
                            currentCount = newList.size();
                            listener.onAddresses(newList, Attributes.EMPTY);
                        }
                    }
                    @Override
                    public void onError(Status error) {
                        synchronized(resolvers) {
                            if(currentCount == 0) listener.onError(error);
                        }
                    }
                });
            }
            @Override
            public String getServiceAuthority() {
                return ETCD;
            }
            @Override
            public void shutdown() {
                synchronized(resolvers) {
                    for(SubResolver sr : resolvers) sr.resolver.shutdown();
                }
            }
        };
    }
    @Override
    public String getDefaultScheme() {
        return ETCD;
    }
    
}
