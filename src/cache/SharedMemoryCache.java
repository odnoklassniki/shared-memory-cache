package cache;

import util.MappedFile;

import java.nio.ByteBuffer;
import java.util.Arrays;


public class SharedMemoryCache implements ICache {
    public static final int KEY_SIZE = 8;
    public static final int DATA_SIZE = 16;
    private static final int ELEMENT_SIZE = KEY_SIZE + DATA_SIZE;

    public final static int MAX_KEY_COUNT(int size) {
        return size / ELEMENT_SIZE;
    }

    private static int dateOffset(int keyAddress) {
        return keyAddress + KEY_SIZE;
    }

    private MappedFile mmap;
    private int segmentSize;
    private int segmentMask;
    private Segment[] segments;

    static final class Segment {
        int start =0;
        int size;
        MappedFile mmap=null;


        Segment(int start, int size, MappedFile mmap) {
            this.size = size ;
            this.start = start;
            this.mmap = mmap;
            verify();
        }

        public  int elementsSpaceSize() {
             return size -4;
        }
        public int elementsSpaceOffset() {
            return start + 4;
        }
        public  int countOffset(){
            return start;
        }

        public int getCount() {
            return  mmap.get(countOffset());
        }

        public void incCount() {
            mmap.put(getCount() + 1, countOffset());
        }


        private void verify() {
            int start = elementsSpaceOffset();
            int size = elementsSpaceSize();
            int pos = start;
            byte[] prevKey = new byte[KEY_SIZE];
            byte[] key = new byte[KEY_SIZE];

            if (getCount() > MAX_KEY_COUNT(size)) {
                throw new Error("shared cache element in position count is invalid");
            }

            for (int c = 0; c < getCount(); c++, pos += ELEMENT_SIZE) {

                if ( mmap.compare(pos, ByteBuffer.wrap(prevKey) ) <= 0 )  {
                    throw new Error("shared cache element in position is invalid");
                }
                mmap.get(prevKey,pos,KEY_SIZE);
            }

            if ( ((pos - start)/ ELEMENT_SIZE) < getCount() ) {
                throw new Error("shared cache element in position count is invalid");
            }

        }


    }

    public SharedMemoryCache(MemoryCacheConfiguration configuration) throws Exception {
        long requestedCapacity = configuration.getCapacity();
        long desiredSegmentSize = configuration.getSegmentSize();
        int  segmentCount = calculateSegmentCount(requestedCapacity, desiredSegmentSize);
        int  segmentSize = (int)((requestedCapacity / segmentCount + 31) & ~31L);

        this.mmap = new MappedFile(configuration.getImageFile(), segmentSize * segmentCount);
        this.segmentSize = (int) segmentSize;
        this.segmentMask = segmentCount - 1;
        this.segments = new Segment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(segmentSize * i, this.segmentSize,mmap);
        }
    }

    @Override
    public void close() {
        mmap.close();
        segments = null;
    }

    @Override
    public byte[] get(byte[] key) {

        if ( key.length != KEY_SIZE) {
            return null;
        }

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            int segmentStart = segment.elementsSpaceOffset();
            int keysEnd = segmentStart + segment.getCount() * ELEMENT_SIZE;
            int keyOffset = binarySearch(key, segmentStart, keysEnd);

            if (keyOffset > 0) {
                int offset = dateOffset(keyOffset);
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
        int keysEnd;
        if (value.length != DATA_SIZE || key.length != KEY_SIZE) {
            return null;
        }

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            int segmentStart = segment.elementsSpaceOffset();
            int count = segment.getCount();
            if (count >= MAX_KEY_COUNT(segment.elementsSpaceSize())) {
                return null;
            }
            //mmap.dump(segment.start,100);
            keysEnd = segmentStart + (count * ELEMENT_SIZE);
            keyOffset = binarySearch(key, segmentStart, keysEnd);
            if (keyOffset < 0) {
                keyOffset = ~keyOffset;
                mmap.copy(keyOffset, keyOffset + ELEMENT_SIZE, keysEnd - keyOffset);
                mmap.put(key, keyOffset, KEY_SIZE);
                segment.incCount();
            }

            mmap.put(value, keyOffset + KEY_SIZE, DATA_SIZE);
            //mmap.dump(segment.start,100);
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

    private int binarySearch(byte[] key, int low, int high) {
        byte[] midval = new byte[KEY_SIZE];
        ByteBuffer kbuf = ByteBuffer.wrap(key);

        for (high -= ELEMENT_SIZE; low <= high; ) {
            int midOffset = low + (((high -low)/ ELEMENT_SIZE) >>> 1) * ELEMENT_SIZE;

            mmap.get(midval,midOffset,KEY_SIZE);
            int compare = mmap.compare(midOffset, kbuf);

            if (compare < 0) {
                low = midOffset + ELEMENT_SIZE;
            } else if (compare > 0) {
                high = midOffset - ELEMENT_SIZE;
            } else {
                return midOffset;
            }
        }
        return ~low;
    }




}
