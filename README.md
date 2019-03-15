[![Build Status](https://travis-ci.org/IBM/etcd-java.svg?branch=master)](https://travis-ci.org/IBM/etcd-java)

# etcd-java

Robust etcd3 java client

- Accepts/exposes protobuf-generated types directly to minimize overhead
- Simpler fluent versions of KV primitives for making async or sync requests
- Automatic retries and (re)authentication
- Resilient and scalable Lease and Watch abstractions with semantics aligned to most typical usage
  - Underlying stream RPCs only kept open for as long as they are needed
- Comes with some helpful [utility classes](utils.md) with local cache, persistent key and leader election abstractions
- Simple declarative client configuration (optional) - encapsulate per-cluster connection details in a
[JSON document](etcd-json-schema.md)
- Support for connecting to multi-endpoint [IBM Compose](https://www.ibm.com/cloud/compose/etcd) etcd deployments over TLS
- Currently doesn't cover all parts of the etcd-server API, in particular those related to cluster administration such as maintenance, cluster and auth management

etcd-java requires Java 8 or higher.

## Usage

Create the client. Methods are grouped into separate `KvClient`, `LeaseClient` and `LockClient` interfaces.


```java
KvStoreClient client = EtcdClient.forEndpoint("localhost", 2379).withPlainText().build();


KvClient kvClient = client.getKvClient();
LeaseClient leaseClient = client.getLeaseClient();
LockClient lockClient = client.getLockClient();

```

Put a key-value synchronously

```java
PutResponse result = kvClient.put(key, value).sync();
```


Get a value asynchronously with timeout

```java
ListenableFuture<RangeResponse> result = kvClient.get(key).timeout(5000).async();
```

Get all key-values with specified prefix


```java
RangeResponse result = kvClient.get(key).asPrefix().sync();
```

Execute a composite transaction asynchronously

```java
ListenableFuture<TxnResponse> result = kvClient.txnIf()
    .cmpEqual(key1).version(123L)
    .and().cmpGreater(key2).mod(456L)
    .and().notExists(key3)
    .then().delete(deleteReq).and().get(rangeReq)
    .elseDo().put(putReq).and().get(rangeReq).async();
```

Batch operations

```java
ListenableFuture<TxnResponse> result = kvClient.batch()
    .put(putReq1).put(putReq2).delete(deleteReq).async();
```

In case of (connection) failure, retry with backoff until successful

```java
ListenableFuture<DeleteRangeResponse> result = kvClient.delete(key)
    .backoffRetry().async();
```

Watch all keys with specified prefix

```java
Watch watch = kvClient.watch(key).asPrefix().start(myWatchObserver);
```

Maintain a persistent lease with a specified id

```java
PersistentLease lease = leaseClient.maintain().leaseId(myId).start();
```

Maintain a persistent lease with server-assigned id and listen for changes

```java
PersistentLease lease = leaseClient.maintain().start(myLeaseObserver);
```

Get all keys equal to or higher than one specified

```java
RangeResponse result = kvClient.get(key).andHigher().sync();
```

Watch _all_ keys in the store, using own executor for update callbacks

```java
Watch watch = kvClient.watch(KvClient.ALL_KEYS)
    .executor(myExecutor).start(myWatchObserver);
```

Watch a key starting from a specific revision, using a _blocking_ iterator

```java
WatchIterator watch = kvClient.watch(key).startRevision(fromRev).start();
```

Obtain a shared persistent lease whose life is tied to the client, and subscribe to session state changes

```java
PersistentLease sessionLease = client.getSessionLease();
sessionLease.addStateObserver(myObserver);

```

Establish a named lock using the client's session lease, then release it

```java
ByteString lockKey = lockClient.lock(name).sync().getKey();
// ...
lockClient.unlock(lockKey).sync();
```

Instantiate a client using a cluster configuration JSON file

```java
KvStoreClient client = EtcdClusterConfig.fromJsonFile(filePath).getClient();
```


## Maven artifact

```xml
<dependency>
    <groupId>com.ibm.etcd</groupId>
    <artifactId>etcd-java</artifactId>
    <version>0.0.9</version>
</dependency>

```

## What about [jetcd](https://github.com/coreos/jetcd)?

etcd-java was originally developed within IBM for production use. At that time jetcd was in a very early and partially-working state; we needed something robust quickly and also had some different ideas for the design and semantics of the API, particularly the watch and lease abstractions.

jetcd has since matured but we still prefer the consumability of etcd-java's API. Now that etcd-java has been open-sourced, it would be great to collaborate on further development and explore the possibility of combining the best of both clients.


The key differences are included in the bulleted feature list at the top of this README.