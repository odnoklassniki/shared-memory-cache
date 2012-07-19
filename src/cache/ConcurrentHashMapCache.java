package cache;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapCache extends ConcurrentHashMap<Long, byte[]> implements ICache {

    public ConcurrentHashMapCache(int capacity, int concurrencyLevel) {
        super(capacity, 0.75f, concurrencyLevel);
    }

    @Override
    public byte[] get(long key) {
        return super.get(key);
    }

    @Override
    public boolean put(long key, byte[] value) {
        super.put(key, value);
        return true;
    }

    @Override
    public void close() {
        clear();
    }
}
