package cache;

import util.MappedFile;

import java.nio.ByteBuffer;
import java.util.Arrays;


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



    // HASH ELEMENT STRUCTURE
    public static final int KEY_REF_SIZE = Short.SIZE/8;
    public static final int KEY_SIZE = 8;
    public static final int LINK_SIZE = Integer.SIZE/8;
    public static final int DATA_SIZE = CacheMetaInfo.DataSize ;
    private static final int ELEMENT_SIZE = LINK_SIZE*2 + KEY_SIZE + DATA_SIZE;



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

        private static int oldLinkOffset(int keyOffset) {
            return  keyOffset + KEY_SIZE ;
        }

        private static int newLinkOffset(int keyOffset) {
            return  keyOffset + KEY_SIZE + LINK_SIZE ;
        }

        private static int dataOffset(int keyOffset) {
            return  keyOffset + KEY_SIZE + 2*LINK_SIZE ;
        }

        private int dataOffset(short ref) {
            return  dataOffset(keyOffset(ref)) ;
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
                if (mmap.compare(keyOffset, KEY_SIZE, ByteBuffer.wrap(prevKey)) <= 0) {
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
                int offset = segment.dataOffset(mmap.getShort(keyRefOffset));
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


            mmap.dump(segment.start,100);
            keysEnd = segmentStart + (segment.getCount()  * KEY_REF_SIZE);
            keyRefOffset = binarySearch(key, segmentStart, keysEnd,segment);
            if (keyRefOffset < 0) {
                if (segment.getCount() >= segment.maxKeyCount()) {
                    return null;
                }
                keyRefOffset = ~keyRefOffset;
                mmap.copy(keyRefOffset, keyRefOffset + KEY_REF_SIZE, keysEnd - keyRefOffset);
                mmap.putShort(segment.getCount(),keyRefOffset);
                keyOffset = segment.keyOffset(segment.getCount());
                mmap.put(key, keyOffset, KEY_SIZE);
                updateTotal(v.size());
                segment.incCount();
            }
            short ref = mmap.getShort(keyRefOffset);
            mmap.put(value, segment.dataOffset(ref),DATA_SIZE);
            moveToHead(LRU_HEAD_OFFSET, ref, segment);
            mmap.dump(segment.start,100);
            return key;
        } finally {
            segment.verify();
            verifyLRU();
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
            int compare = mmap.compare(midOffset,KEY_SIZE, kbuf);

            if (compare < 0) {
                low = midRefOffset + KEY_REF_SIZE;
            } else if (compare > 0) {
                high = midRefOffset - KEY_REF_SIZE;
            } else {
                return midRefOffset;
            }
        }
        return ~low;
    }

    private int prev(int offset ){
        if (offset == LRU_HEAD_OFFSET ) {
           return  mmap.get(offset);
        }
        else {
            return mmap.get(Segment.oldLinkOffset(offset));
        }
    }

    private void removeLink(int headLinkOffest, int keyOffset) {
        int oldKeyOffset = mmap.get(Segment.oldLinkOffset(keyOffset));
        if (oldKeyOffset > 0) {
            int newKeyOffset = mmap.get(Segment.newLinkOffset(keyOffset));
            int headKeyOffset = mmap.get(headLinkOffest);
            if (keyOffset == headKeyOffset) {
                mmap.put(headKeyOffset, Segment.newLinkOffset(oldKeyOffset));
                mmap.put(oldKeyOffset, headLinkOffest );
            } else {
                mmap.put(newKeyOffset, Segment.newLinkOffset(oldKeyOffset));
                mmap.put(oldKeyOffset, Segment.oldLinkOffset(newKeyOffset) );
            }
        }
        mmap.put(0, Segment.oldLinkOffset(keyOffset));
        mmap.put(0, Segment.newLinkOffset(keyOffset));
    }


    private void updateHead(int headLinkOffest, int keyOffset) {
        int headKeyOffset = mmap.get(headLinkOffest);
        if (keyOffset != headKeyOffset) {
            if (headKeyOffset > 0) {
                mmap.put(keyOffset, Segment.newLinkOffset(headKeyOffset));
                mmap.put(headKeyOffset, Segment.oldLinkOffset(keyOffset));
            }
            mmap.put(keyOffset, headLinkOffest);
        }
    }

    private void moveToHead(int headLinkOffest, short ref, Segment seg) {
        int keyOffset = seg.keyOffset(ref);
        if (keyOffset != mmap.get(headLinkOffest)) {
            removeLink(headLinkOffest, keyOffset);
            updateHead(headLinkOffest, keyOffset);
        }
    }


    private void verifyLRU() {
        int keyOffset = LRU_HEAD_OFFSET;
        int count = 0;
        while ( (keyOffset = prev(keyOffset)) > 0) {
         count++;
        }
        if (count != this.count()) {
            throw new Error("shared cache - not all keys are linked");
        }
    }




}
