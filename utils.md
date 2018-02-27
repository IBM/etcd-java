# Utilities built on etcd-java

Included in the `com.ibm.etcd.client.utils` package.

- `RangeCache`
- `PersistentLeaseKey`
- `EtcdLeaderElection`

### [RangeCache](src/main/java/com/ibm/etcd/client/utils/RangeCache.java)

Auto-updated write-through lock-free cache for etcd3, supporting atomic compare-and-set updates. Provides local sequential consistency between writes and reads, similar to the happens-before guarantee of a java `ConcurrentHashMap`.

### [PersistentLeaseKey](src/main/java/com/ibm/etcd/client/utils/PersistentLeaseKey.java)

Etcd key-value bound to a [PersistentLease](src/main/java/com/ibm/etcd/client/lease/PersistentLease.java). If the key already exists it's value won't be changed but it will be associated with the provided lease. If it doesn't already exist or is deleted by someone else, it will be (re)-created with a provided default value. Closing the PersistentLeaseKey will always delete the associated key-value.

### [EtcdLeaderElection](src/main/java/com/ibm/etcd/client/utils/EtcdLeaderElection.java)

Etcd-based leader election pattern. Supports attachment of listeners for notification of leadership change, and an "observer" mode where leadership can be monitored without participating in the election.


`PersistentLeaseKey` and `RangeCache` can be used together for service registration and discovery.