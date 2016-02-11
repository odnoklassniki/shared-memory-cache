package cache;

import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentHashMapCache extends ConcurrentHashMap<byte[], byte[]> implements ICache {

    public ConcurrentHashMapCache(int capacity, int concurrencyLevel) {
        super(capacity, 0.75f, concurrencyLevel);
    }

    @Override
    public byte[] get(byte[]key) {
        return super.get(key);
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
       return  super.put(key, value) ;
    }

    @Override
    public void close() {
        clear();
    }
}
