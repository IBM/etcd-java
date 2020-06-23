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

import static io.grpc.EquivalentAddressGroup.ATTR_AUTHORITY_OVERRIDE;
import static java.util.stream.Collectors.toList;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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

    static class SubResolver {
        final NameResolver resolver;
        List<EquivalentAddressGroup> eagList = Collections.emptyList();

        public SubResolver(URI uri, NameResolver.Args args) {
            this.resolver = DNS_PROVIDER.newNameResolver(uri, args);
        }

        void updateEagList(List<EquivalentAddressGroup> servers, boolean ownAuthority) {
            if (ownAuthority) {
                // Use this endpoint address' authority for its subchannel
                String authority = resolver.getServiceAuthority();
                eagList = servers.stream().map(eag -> new EquivalentAddressGroup(
                        eag.getAddresses(), eag.getAttributes().toBuilder()
                            .set(ATTR_AUTHORITY_OVERRIDE, authority).build())).collect(toList());
            } else {
                eagList = servers;
            }
        }
    }

    private final URI[] uris;
    private final String overrideAuthority;

    public StaticEtcdNameResolverFactory(List<String> endpoints) {
        this(endpoints, null);
    }

    public StaticEtcdNameResolverFactory(List<String> endpoints, String overrideAuthority) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints");
        }
        this.overrideAuthority = overrideAuthority;
        uris = endpoints.stream()
                .map(ep -> URI.create(EtcdClient.endpointToUriString(ep)))
                .toArray(URI[]::new);
        if (uris.length > 1) {
            Collections.shuffle(Arrays.asList(uris));
        }
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        if (!ETCD.equals(targetUri.getScheme())) {
            return null;
        }
        if (uris.length == 1) {
            return new SubResolver(uris[0], args).resolver;
        }
        SubResolver[] resolvers = createSubResolvers(args);
        return new NameResolver() {
            int currentCount = 0;
            @Override
            public void start(Listener listener) {
                for (SubResolver sr : resolvers) sr.resolver.start(new Listener() {
                    @Override
                    public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
                        synchronized (resolvers) {
                            // Update this subresolver's servers
                            sr.updateEagList(servers, overrideAuthority == null);
                            // Advertise the complete list of EAGs
                            List<EquivalentAddressGroup> newList = Stream.of(resolvers)
                                    .flatMap(r -> r.eagList.stream()).collect(toList());
                            currentCount = newList.size();
                            listener.onAddresses(newList, Attributes.EMPTY);
                        }
                    }
                    @Override
                    public void onError(Status error) {
                        synchronized (resolvers) {
                            if (currentCount == 0) {
                                listener.onError(error);
                            }
                        }
                    }
                });
            }
            @Override
            public void refresh() {
                synchronized (resolvers) {
                    for (SubResolver sr : resolvers) {
                        sr.resolver.refresh();
                    }
                }
            }
            @Override
            public String getServiceAuthority() {
                return overrideAuthority != null ? overrideAuthority : ETCD;
            }
            @Override
            public void shutdown() {
                synchronized (resolvers) {
                    for (SubResolver sr : resolvers) {
                        sr.resolver.shutdown();
                    }
                }
            }
        };
    }

    private SubResolver[] createSubResolvers(NameResolver.Args args) {
        int count = uris.length;
        SubResolver[] resolvers = new SubResolver[count];
        for (int i = 0; i < count; i++) {
            resolvers[i] = new SubResolver(uris[i], args);
        }
        return resolvers;
    }

    @Override
    public String getDefaultScheme() {
        return ETCD;
    }
}
