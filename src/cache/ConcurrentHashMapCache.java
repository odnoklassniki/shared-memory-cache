package cache;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapCache extends ConcurrentHashMap<byte[], SharedMemoryCache.CacheMetaInfo> implements ICache {

    public ConcurrentHashMapCache(int capacity, int concurrencyLevel) {
        super(capacity, 0.75f, concurrencyLevel);
    }

    @Override
    public SharedMemoryCache.CacheMetaInfo get(byte[]key) {
        return super.get(key);
    }

    @Override
    public SharedMemoryCache.CacheMetaInfo  put(byte[] key, SharedMemoryCache.CacheMetaInfo value) {
       return  super.put(key, value) ;
    }

    @Override
    public void close() {
        clear();
    }
}
