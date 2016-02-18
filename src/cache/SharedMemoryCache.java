package cache;

import util.MappedFile;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;


public class SharedMemoryCache implements ICache {

    // Attributes
    private MappedFile mmap;
    private int segmentSize;
    private int segmentMask;
    private Segment[] segments;
    private byte aKey[];
    private byte aData[];

    // FILE HEADER STRUCTURE
    public static final int LINK_SIZE = Integer.SIZE/8;
    public static final int TOTAL_SIZE_OFFSET = 0;
    public static final int LIMIT_SIZE_OFFSET = TOTAL_SIZE_OFFSET + Long.SIZE/8;
    public static final int LRU_HEAD_OFFSET   = LIMIT_SIZE_OFFSET + Long.SIZE/8;
    public static final int LRU_TAIL_OFFSET   = LRU_HEAD_OFFSET   + Long.SIZE/8;
    public static final int HEADER_SIZE       = LRU_TAIL_OFFSET   + LINK_SIZE;

    private long getTotal() {
        return mmap.getLong(TOTAL_SIZE_OFFSET);
    }

    private void updateTotal(long size) {
        mmap.putLong(getTotal() + size, TOTAL_SIZE_OFFSET);
    }

    private long getLimit() {
        return mmap.getLong(LIMIT_SIZE_OFFSET);
    }

    private void setLimit(long limit) {
        mmap.putLong(limit, LIMIT_SIZE_OFFSET);
    }



    // HASH ELEMENT STRUCTURE
    public static final int KEY_REF_SIZE = Short.SIZE/8;
    public static final int KEY_SIZE = 8;
    public static final int DATA_SIZE = CacheMetaInfo.DataSize ;
    private static final int ELEMENT_SIZE = KEY_SIZE+  LINK_SIZE*2 + DATA_SIZE;



    public static final class CacheMetaInfo {

        ByteBuffer bytes_;

        public static final int activateOffset = 0;
        public static final int sizeOffset = 1;
        public static final int timeOffset = Long.SIZE/8 + 1;
        public static final int DataSize = Long.SIZE/8 + Long.SIZE/8 + 1;


        public CacheMetaInfo(ByteBuffer bytes) {
            bytes_ = bytes;
        }

        public CacheMetaInfo(byte activate, long size) {
            bytes_ = ByteBuffer.allocate(DataSize);
            bytes_.put(activate);
            bytes_.putLong(size);
            bytes_.putLong(0);
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

        public void setTimestamp() {
            bytes_.putLong(timeOffset, System.nanoTime());
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

            // init refs for future usage
            if (getCount() == 0 ) {
                for (int c = 0,pos = start; c <= maxKeyCount(); c++, pos += KEY_REF_SIZE) {
                    mmap.putShort((short)c,pos);
                }
            }
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

        public void decCount() {
            mmap.putShort((short)(getCount()-1), countOffset());
        }

        public void clear() {
            mmap.putShort((short)0, countOffset());
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

        private  boolean active(int keyOffset) {
            return  mmap.getByte(dataOffset(keyOffset)) > 0 ;
        }

        private int keysRefEnd() {
            return elementsSpaceOffset() + (getCount() * KEY_REF_SIZE);
        }

        private  CacheMetaInfo data(int keyOffset) {
            byte[] data = new byte[DATA_SIZE];
            mmap.get(data, dataOffset(keyOffset),DATA_SIZE) ;
            return new CacheMetaInfo(ByteBuffer.wrap(data));
        }

        private int dataOffset(short ref) {
            return  dataOffset(keyOffset(ref)) ;
        }


        public int binarySearch(byte[] key) {

            int low = elementsSpaceOffset();
            int high = low + (getCount()  * KEY_REF_SIZE);
            ByteBuffer kbuf = ByteBuffer.wrap(key);

            for (high -= KEY_REF_SIZE; low <= high; ) {
                int midRefOffset = low + (((high - low) / KEY_REF_SIZE) >>> 1) * KEY_REF_SIZE;
                short midRef = mmap.getShort(midRefOffset);
                int midOffset = keyOffset(midRef);

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

        private boolean verify() {
            int start = elementsSpaceOffset();

            int keyOffset;
            byte[] prevKey = new byte[KEY_SIZE];

            if (getCount() > maxKeyCount()) {
                //throw new Error("shared cache element in position count is invalid");
                return false;
            }

            for (int c = 0,pos = start; c < getCount(); c++, pos += KEY_REF_SIZE) {
                keyOffset =   this.keyOffset(mmap.getShort(pos));
                if (mmap.compare(keyOffset, KEY_SIZE, ByteBuffer.wrap(prevKey)) <= 0) {
                    //throw new Error("shared cache element in position is invalid");
                    return false;
                }
                mmap.get(prevKey, keyOffset, KEY_SIZE);
            }



            return true;
        }

    }

    public SharedMemoryCache(MemoryCacheConfiguration configuration) throws Exception {
        long requestedCapacity = configuration.getCapacity() - HEADER_SIZE;
        long desiredSegmentSize = configuration.getSegmentSize();
        int segmentCount = calculateSegmentCount(requestedCapacity, desiredSegmentSize);
        int segmentSize = (int) ((requestedCapacity / segmentCount + 31) & ~31L);
        this.aKey = new byte[KEY_SIZE];
        this.aData = new byte[DATA_SIZE];
        this.mmap = new MappedFile(configuration.getImageFile(), segmentSize * segmentCount);
        mmap.lock();

        this.segmentSize = (int) segmentSize;
        this.segmentMask = segmentCount - 1;
        this.segments = new Segment[segmentCount];

        for (int i = 0; i < segmentCount; i++) {
            segments[i] = new Segment(HEADER_SIZE + segmentSize * i, this.segmentSize, mmap);
        }
        if ( this.getLimit() == 0  ) {
            this.setLimit(configuration.getLimit());
        }

        if (!verify()) {
            throw new Error("need to be scanned all over");
        }

        mmap.release();
    }

    @Override
    public void close() {
        mmap.close();
        segments = null;
    }

    @Override
    public CacheMetaInfo get(byte[] key) {
        int  keyOffset;
        if (key.length != KEY_SIZE) {
            return null;
        }

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            if ((keyOffset = getKeyOffset(key)) > 0) {
                int offset = segment.dataOffset(keyOffset);
                byte[] result = new byte[DATA_SIZE];
                mmap.get(result, offset, DATA_SIZE);
                return new CacheMetaInfo(ByteBuffer.wrap(result));
            }
            return null;
        } finally {
            mmap.release();
        }
    }


    private int getKeyOffset(byte[] key) {
        int keyOffset = 0;
        Segment segment = segmentFor(key);
        int keyRefOffset = segment.binarySearch(key);

        if (keyRefOffset > 0) {
            keyOffset = segment.keyOffset(mmap.getShort(keyRefOffset));
        }
        return keyOffset;
    }

    public boolean deactivate(byte[] key) {
        int  keyOffset;
        if (key.length != KEY_SIZE) {
            return false;
        }

        Segment segment = segmentFor(key);
        mmap.lock();
        try {
            if ((keyOffset = getKeyOffset(key)) > 0) {
                int offset = segment.dataOffset(keyOffset) + CacheMetaInfo.activateOffset;
                mmap.putByte((byte) 0, offset);
                return true;
            }
            return false;
        } finally {
            mmap.release();
        }
    }


    public boolean verifyOrClear() {
        boolean verified = true;
        mmap.lock();
        try {
          if (!verify()) {
              clear();
              verified = false;
          }
        } finally {
            mmap.release();
        }
        return verified;
    }


    public void delete(int keyOffset) {
        Segment segment = segmentFor(keyOffset);
        mmap.get(aKey,keyOffset,KEY_SIZE);
        removeLink(keyOffset);
        updateTotal(-1*mmap.getLong(segment.dataOffset(keyOffset)+CacheMetaInfo.sizeOffset));
        int keyRefOffset = segment.binarySearch(aKey);
        if ( keyOffset < 0 ) {
            throw new Error ("must have the deleted key");
        }
        short ref = mmap.getShort(keyRefOffset);
        int keyRefsEnd = segment.keysRefEnd();
        mmap.copy(keyRefOffset + KEY_REF_SIZE, keyRefOffset,keyRefsEnd - keyRefOffset - KEY_REF_SIZE);
        mmap.putShort(ref,keyRefsEnd-KEY_REF_SIZE);
        mmap.clear(keyOffset,ELEMENT_SIZE);
        segment.decCount();
    }

    public void clear() {
        mmap.put(0, LRU_HEAD_OFFSET);
        mmap.put(0, LRU_HEAD_OFFSET);
        mmap.putLong(0L, TOTAL_SIZE_OFFSET);
        for (Segment segment : segments) {
            segment.clear();
        }
    }


    public boolean needEviction(long newSize) {  return (this.getTotal() + newSize - this.getLimit()) > 0; }

    public boolean evict(long newSize) {
        int keyOffset = LRU_TAIL_OFFSET;
        Segment seg;

        while (needEviction(newSize) && (keyOffset = next(keyOffset)) > 0) {
            seg = segmentFor(keyOffset);
            if (!seg.active(keyOffset)) {
                delete(keyOffset);
            }
        }
        return !needEviction(newSize);
    }


    @Override
    public CacheMetaInfo put(byte[] key, CacheMetaInfo value) {
        int keyOffset;
        int keyRefOffset;
        int keyRefsEnd;
        short newRef;

        if (value.bytes().length != DATA_SIZE || key.length != KEY_SIZE) {
            return null;
        }


        Segment segment = segmentFor(key);
        mmap.lock();
        value.setTimestamp();
        try {
          //  mmap.dump(segment.start,100);
            keyRefOffset = segment.binarySearch(key);
            if (keyRefOffset < 0) {
                if (segment.getCount() >= segment.maxKeyCount()) {
                    return null;
                }

                if (needEviction(value.size())) {
                    if (!evict(value.size())) {
                        return null;
                    }
                    // in case evicted from the same segment
                    keyRefOffset = segment.binarySearch(key);
                }

                keyRefOffset = ~keyRefOffset;
                keyRefsEnd = segment.keysRefEnd();
                newRef = mmap.getShort(keyRefsEnd);
                mmap.copy(keyRefOffset, keyRefOffset + KEY_REF_SIZE, keyRefsEnd - keyRefOffset);
                mmap.putShort(newRef,keyRefOffset);
                keyOffset = segment.keyOffset(newRef);
                mmap.put(key, keyOffset, KEY_SIZE);
                mmap.put(0, Segment.oldLinkOffset(keyOffset));
                mmap.put(0, Segment.newLinkOffset(keyOffset));
                updateTotal(value.size());
                segment.incCount();
            }
            short ref = mmap.getShort(keyRefOffset);
            mmap.put(value.bytes(), segment.dataOffset(ref),DATA_SIZE);
            moveToHead(ref, segment);
         //   mmap.dump(segment.start,100);
        } finally {
            if (!verify()) {
                mmap.release();
                return null;
            }
            mmap.release();
        }

        return value;
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

    private Segment segmentFor(int keyOffset) {
         int index = (keyOffset -HEADER_SIZE)/segmentSize;
         return segments[index];
    }



    private int next(int offset){
        if (offset == LRU_TAIL_OFFSET) {
           return  mmap.get(offset);
        }
        else {
            return mmap.get(Segment.newLinkOffset(offset));
        }
    }

    private void removeLink(int keyOffset) {
        int oldKeyOffset = mmap.get(Segment.oldLinkOffset(keyOffset));
        int newKeyOffset = mmap.get(Segment.newLinkOffset(keyOffset));
        int tailKeyOffset = mmap.get(LRU_TAIL_OFFSET);
        int headKeyOffset = mmap.get(LRU_HEAD_OFFSET);
        if (keyOffset == headKeyOffset) {
            if (oldKeyOffset > 0) {
                mmap.put(headKeyOffset, Segment.newLinkOffset(oldKeyOffset));
            }
            mmap.put(oldKeyOffset, LRU_HEAD_OFFSET);
        }

        if (keyOffset == tailKeyOffset) {
            if (newKeyOffset > 0) {
                mmap.put(tailKeyOffset, Segment.oldLinkOffset(newKeyOffset));
            }
            mmap.put(newKeyOffset, LRU_TAIL_OFFSET);
        }

        if ( !(keyOffset == tailKeyOffset || keyOffset == headKeyOffset) ) {
            if (oldKeyOffset > 0) {
                mmap.put(newKeyOffset, Segment.newLinkOffset(oldKeyOffset));
            }
            if (newKeyOffset > 0) {
                mmap.put(oldKeyOffset, Segment.oldLinkOffset(newKeyOffset));
            }
        }
        mmap.put(0, Segment.oldLinkOffset(keyOffset));
        mmap.put(0, Segment.newLinkOffset(keyOffset));
    }


    private void updateHead(int keyOffset) {
        int headKeyOffset = mmap.get(LRU_HEAD_OFFSET);
        if (keyOffset != headKeyOffset) {
            if (headKeyOffset > 0) {
                mmap.put(keyOffset, Segment.newLinkOffset(headKeyOffset));
                mmap.put(headKeyOffset, Segment.oldLinkOffset(keyOffset));
            }
            mmap.put(keyOffset, LRU_HEAD_OFFSET);
        }
    }

    private void updateEmptyTail(int keyOffset) {
        int tailKeyOffset = mmap.get(LRU_TAIL_OFFSET);
        if (tailKeyOffset == 0) {
            mmap.put(keyOffset, LRU_TAIL_OFFSET);
        }
    }

    private void moveToHead(short ref, Segment seg) {
        int keyOffset = seg.keyOffset(ref);
        if (keyOffset != mmap.get(LRU_HEAD_OFFSET)) {
            removeLink(keyOffset);
            updateHead(keyOffset);
        }
        updateEmptyTail(keyOffset);
    }


    private boolean verify() {
        int keyOffset = LRU_TAIL_OFFSET;
        int nextKeyOffset;
        int count = 0;
        long totalSize = 0;
        Segment seg;
        CacheMetaInfo info = new CacheMetaInfo((byte)0,0L);
        CacheMetaInfo nextInfo;

        try {
            while ((nextKeyOffset = next(keyOffset)) > 0) {
                seg = segmentFor(nextKeyOffset);
                nextInfo = seg.data(nextKeyOffset);
                if (info.timestamp() > nextInfo.timestamp()) {
                    return false;
                }
                count++;
                info = nextInfo;
                totalSize += info.size();
                keyOffset = nextKeyOffset;

            }
            if (count != this.count()) {
                return false;
            }

            if (totalSize != this.getTotal()) {
                return false;
            }

            for (Segment segment : segments) {
                if (!segment.verify()) {
                    return false;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }




}
