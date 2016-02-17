package cache;

public interface ICache {
    SharedMemoryCache.CacheMetaInfo get(byte[] key);
    SharedMemoryCache.CacheMetaInfo put(byte[]key, SharedMemoryCache.CacheMetaInfo value);
    void close();
}
