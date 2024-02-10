## About:

Experimental implementation of a persistent key-value store written with
Seastar library.

## Dependencies:

Seastar
nlohmann_json

## Test dependencies:

curl

## TODO:

cmake integration tests for --memory_threshold 1 to test persistent store
cmake unit tests for sstables (and the rest...)
rest of LSM tree support (compaction, bloom filters)
utf8 key normalization (perhaps use libutf8proc-dev)
remote shards support (horizontal scaling)
swagger documentation
compression of keys and values on server side
compression of values on client side (submitting compressed via REST api)
explore faster/better distributed hashing functions
single file with multiple values for intermediate sized values
keys request paging an consider how to prevent sorting in memory
checksums to make sure content is valid instead of just relying on the filesystem
make sure that data is actually persisted on disk and not just in write cache
sstable skip indexes (in memory part of sstable file that allows faster jumps through the file whien searching)
configurable db storage path
deletion of orphan value files in sstables (can occur because of app terminations)
