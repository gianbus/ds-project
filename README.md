# ds-project: A quorum-based replicated datastore

This is the implementation of a distributed key-value store that accepts two operations from clients:
* put(k, v) inserts/updates value v for key k;
* get(k) returns the value associated with key k (or null if the key is not present).

The store is internally replicated across N nodes (processes) and offers sequential consistency using a quorum-based protocol. The read and write quorums are configuration parameters.
