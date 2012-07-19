package cache;

public class MemoryCacheConfiguration {
    private long capacity;
    private long segmentSize;
    private String imageFile;

    public MemoryCacheConfiguration(long capacity, long segmentSize, String imageFile) {
        this.capacity = capacity;
        this.segmentSize = segmentSize;
        this.imageFile = imageFile;
    }

    public long getCapacity() {
        return capacity;
    }

    public long getSegmentSize() {
        return segmentSize;
    }

    public String getImageFile() {
        return imageFile;
    }
}
