package cache;

public class MemoryCacheConfiguration {
    private long capacity;
    private long segmentSize;
    private String imageFile;
    private long limit;

    public MemoryCacheConfiguration(long capacity, long segmentSize, long limit, String imageFile) {
        this.capacity = capacity;
        this.segmentSize = segmentSize;
        this.imageFile = imageFile;
        this.limit = limit;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public long getLimit() {
        return limit;
    }

    public String getImageFile() {
        return imageFile;
    }


}
