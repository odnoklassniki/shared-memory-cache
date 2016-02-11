package cache;

public interface ICache {
    byte[] get(byte[] key);
    byte[] put(byte[]key, byte[] value);
    void close();
}
