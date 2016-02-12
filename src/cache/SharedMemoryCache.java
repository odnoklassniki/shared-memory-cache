package cache;

import sun.jvm.hotspot.runtime.Bytes;
import util.MappedFile;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;


public class SharedMemoryCache implements ICache {

    // Attributes
    private MappedFile mmap;
    private int segmentSize;
    private int segmentMask;
    private Segment[] segments;

    // FILE HEADER STRUCTURE
    public static final int TOTAL_SIZE_OFFSET = 0;
    public static final int LRU_HEAD_OFFSET = TOTAL_SIZE_OFFSET + 8;
    public static final int HEADER_SIZE = LRU_HEAD_OFFSET + 4;

    private long getTotal() {
        return mmap.getLong(TOTAL_SIZE_OFFSET);
    }

    private void updateTotal(long size) {
        mmap.putLong(getTotal() + size, TOTAL_SIZE_OFFSET);
    }

    private void updateLRU(int offsetLatest) {

    }

    // HASH ELEMENT STRUCTURE
    public static final int KEY_REF_SIZE = Short.SIZE/8;
    public static final int KEY_SIZE = 8;
    public static final int DATA_SIZE = CacheMetaInfo.DataSize ;
    private static final int ELEMENT_SIZE = KEY_SIZE + DATA_SIZE;






    static final class CacheMetaInfo {

        ByteBuffer bytes_;

        public static final int activateOffset = 0;
        public static final int sizeOffset = 1;
        public static final int timeOffset = Long.SIZE/8 + 1;
        public static final int DataSize = Long.SIZE/8 + Long.SIZE/8 + 1;

        CacheMetaInfo(ByteBuffer bytes) {
            bytes_ = bytes;
        }

        CacheMetaInfo(byte activate, long size, long timestamp) {
            bytes_ = ByteBuffer.allocate(DataSize);
            bytes_.put(activate);
            bytes_.putLong(timestamp);
            bytes_.putLong(size);

        }

        public byte[] bytes() { return bytes_.array(); }

        public boolean activated() {
            return bytes_.get(activateOffset) > 0;
        }

        public long timestamp() {
            bytes_.position(timeOffset);
            bytes_.mark();
            long val =  bytes_.getLong();
            bytes_.reset();
            return val;
        }

        public long size() {
            bytes_.position(sizeOffset);
            bytes_.mark();
            long val = bytes_.getLong();
            bytes_.reset();
            return val;
        }



    }


    static final class Segment {
        int start = 0;
        int size;
        MappedFile mmap = null;


        Segment(int start, int size, MappedFile mmap) {
            this.size = size;
            this.start = start;
            this.mmap = mmap;
            verify();
        }

        public int elementsSpaceSize() {
            return size - 2;
        }

        public int elementsSpaceOffset() {
            return start + 2;
        }

        public int countOffset() {
            return start;
        }

        public short getCount() {
            return mmap.getShort(countOffset());
        }

        public void incCount() {
            mmap.putShort((short)(getCount()+1), countOffset());
        }


        public final int maxKeyCount() {
            int numOfElements = elementsSpaceSize() / (ELEMENT_SIZE + KEY_REF_SIZE);
            if ( numOfElements >  Short.MAX_VALUE) {
                numOfElements =  Short.MAX_VALUE;
                System.out.printf("LOG OF ERROR");
            }
            return numOfElements;
        }

        private int keysOffset() {
            return  elementsSpaceOffset()+  maxKeyCount()*KEY_REF_SIZE;
        }

        private int keyOffset(short ref) {
            return keysOffset() + ref * ELEMENT_SIZE;
        }

        private int dateOffset(short ref) {
            return  keyOffset(ref) + DATA_SIZE ;
        }

        private void verify() {
            int start = elementsSpaceOffset();
            int size = elementsSpaceSize();
            int pos = start;
            int keyOffset;
            byte[] prevKey = new byte[KEY_SIZE];
            byte[] key = new byte[KEY_SIZE];

            if (getCount() > maxKeyCount()) {
                throw new Error("shared cache element in position count is invalid");
            }

            for (int c = 0; c < getCount(); c++, pos += KEY_REF_SIZE) {
                keyOffset =   this.keyOffset(mmap.getShort(pos));
                if (mmap.compare(keyOffset, ByteBuffer.wrap(prevKey)) <= 0) {
                    throw new Error("shared cache element in position is invalid");
                }
                mmap.get(prevKey, keyOffset, KEY_SIZE);
            }
        }
    }

    public SharedMemoryCache(MemoryCacheConfiguration configuration) throws Exception {
        long requestedCapacity = configuration.getCapacity() - HEADER_SIZE;
        long desiredSegmentSize = configuration.getSegmentSize();
        int segmentCount = calculateSegmentCount(requestedCapacity, desiredSegmentSize);
        int segmentSize = (int) ((requestedCapacity / segmentCount + 31) & ~31L);

        this.mmap = new MappedFile(configuration.getImageFile(), segmentSize * segmentCount);
        this.segmentSize = (int) segmentSize;
        this.segmentMask = segmentCount - 1;
        this.segments = new Segment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(HEADER_SIZE + segmentSize * i, this.segmentSize, mmap);
        }
    }

    @Override
    public void close() {
        mmap.close();
        segments = null;
    }

    @Override
    public byte[] get(byte[] key) {

        if (key.length != KEY_SIZE) {
            return null;
        }

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            int segmentStart = segment.elementsSpaceOffset();
            int keysEnd = segmentStart + segment.getCount() * KEY_REF_SIZE;
            int keyRefOffset = binarySearch(key, segmentStart, keysEnd,segment);

            if (keyRefOffset > 0) {
                int offset = segment.dateOffset(mmap.getShort(keyRefOffset));
                byte[] result = new byte[DATA_SIZE];
                mmap.get(result, offset, DATA_SIZE);
                return result;
            }

            return null;
        } finally {
            mmap.release();
        }
    }


    @Override
    public byte[] put(byte[] key, byte[] value) {
        int keyOffset;
        int keyRefOffset;
        int keysEnd;
        if (value.length != DATA_SIZE || key.length != KEY_SIZE) {
            return null;
        }

        CacheMetaInfo v = new  CacheMetaInfo(ByteBuffer.wrap(value));

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            int segmentStart = segment.elementsSpaceOffset();
            int count = segment.getCount();
            if (count >= segment.maxKeyCount()) {
                return null;
            }
            mmap.dump(segment.start,100);
            keysEnd = segmentStart + (count * KEY_REF_SIZE);
            keyRefOffset = binarySearch(key, segmentStart, keysEnd,segment);
            if (keyRefOffset < 0) {
                keyRefOffset = ~keyRefOffset;
                mmap.copy(keyRefOffset, keyRefOffset + KEY_REF_SIZE, keysEnd - keyRefOffset);
                mmap.putShort(segment.getCount(),keyRefOffset);
                keyOffset = segment.keyOffset(segment.getCount());
                mmap.put(key, keyOffset, KEY_SIZE);
                updateTotal(v.size());
                segment.incCount();
            }
            keyOffset = segment.keyOffset(mmap.getShort(keyRefOffset));
            mmap.put(value, keyOffset + KEY_SIZE, DATA_SIZE);
            mmap.dump(segment.start,100);
            return key;
        } finally {
            segment.verify();
            mmap.release();
        }
    }


    public int count() {
        int count = 0;
        for (Segment segment : segments) {
            count += segment.getCount();
        }
        return count;
    }

    private int calculateSegmentCount(long requestedCapacity, long segmentSize) {
        int segmentCount = 1;
        while (segmentSize * segmentCount < requestedCapacity) {
            segmentCount <<= 1;
        }
        return segmentCount;
    }

    private Segment segmentFor(byte[] key) {
        return segments[Arrays.hashCode(key) & segmentMask];
    }

    private int binarySearch(byte[] key, int low, int high, Segment seg) {
        byte[] midval = new byte[KEY_SIZE];
        ByteBuffer kbuf = ByteBuffer.wrap(key);

        for (high -= KEY_REF_SIZE; low <= high; ) {
            int midRefOffset = low + (((high - low) / KEY_REF_SIZE) >>> 1) * KEY_REF_SIZE;
            short midRef = mmap.getShort(midRefOffset);
            int midOffset = seg.keyOffset(midRef);
            mmap.get(midval, midOffset, KEY_SIZE);
            int compare = mmap.compare(midOffset, kbuf);

            if (compare < 0) {
                low = midRefOffset + KEY_REF_SIZE;
            } else if (compare > 0) {
                high = midRefOffset - KEY_REF_SIZE;
            } else {
                return midOffset;
            }
        }
        return ~low;
    }


}
