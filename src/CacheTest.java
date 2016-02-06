import cache.*;

import java.util.Random;

public class CacheTest {
    private static final long K = 1024;
    private static final long M = 1024*K;
    private static final long G = 1024*M;

    private static final long MAGIC = 54331;

    private static final int WARMUP_COUNT = 100000;
    private static final int RUN_COUNT    = 1000000;
    private static final int DATA_SIZE    = 256;

    public static void testWrite(ICache cache, int count) {
        Random random = new Random(0);
        for (int i = 0; i < count; i++) {
            long key = random.nextInt(1 << 20) * MAGIC;
            cache.put(key, new byte[random.nextInt(DATA_SIZE)]);
        }
    }

    public static void testRead(ICache cache, int count) {
        Random random = new Random(1);
        for (int i = 0; i < count; i++) {
            long key = random.nextInt(1 << 20) * MAGIC;
            cache.get(key);
        }
    }

    public static void testRead9Write1(ICache cache, int count) {
        Random random = new Random(2);
        for (int i = 0; i < count; i++) {
            long key = random.nextInt(1 << 20) * MAGIC;
            if (random.nextInt(10) == 0) {
                cache.put(key, new byte[random.nextInt(DATA_SIZE)]);
            } else {
                cache.get(key);
            }
        }
    }

    public static void testAll(ICache cache) {
        // Warm-up
        testWrite(cache, WARMUP_COUNT);
        testRead(cache, WARMUP_COUNT);
        testRead9Write1(cache, WARMUP_COUNT);

        String cacheClass = cache.getClass().getSimpleName();
        long start, end;

        start = System.currentTimeMillis();
        testWrite(cache, RUN_COUNT);
        end = System.currentTimeMillis();
        System.out.println(cacheClass + " write: " + (end - start));

        start = System.currentTimeMillis();
        testRead(cache, RUN_COUNT);
        end = System.currentTimeMillis();
        System.out.println(cacheClass + " read: " + (end - start));

        start = System.currentTimeMillis();
        testRead9Write1(cache, RUN_COUNT);
        end = System.currentTimeMillis();
        System.out.println(cacheClass + " read-write: " + (end - start));
    }




    public static void main(String[] args) throws Exception {
        String type = args.length == 0 ? null : args[0];
        ICache cache;
        if ("chm".equals(type)) {
            cache = new ConcurrentHashMapCache(3000000, 256);
        } else {
            cache = new SharedMemoryCache(new MemoryCacheConfiguration(200*M, 50*K, "/tmp/cache-shm-test"));
        }
        testAll(cache);
        cache.close();
    }
}
