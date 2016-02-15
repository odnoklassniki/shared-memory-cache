package util;

import sun.nio.ch.FileChannelImpl;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;


public class MappedFile {
    private FileChannel ch;
    private FileLock lock;
    private MappedByteBuffer map;

    public MappedFile(String name, long size) throws Exception {
        RandomAccessFile f = null;
        size = (size + 0xfffL) & ~0xfffL;
        f = new RandomAccessFile(name, "rw");
        f.setLength(size);
        ch = f.getChannel();
        map = ch.map(FileChannel.MapMode.READ_WRITE, 0, size);
    }

    public void put(byte[] buffer, int index, int length) {
        map.position(index);
        map.put(buffer, 0, length);
    }

    public void get(byte[] buffer, int index, int length) {
        map.position(index);
        map.mark();
        map.get(buffer, 0, length);
        map.reset();
    }


    public void copy(int srcIndex, int targetIndex, int length) {
        map.position(0);
        for (int i = length-1; i >= 0; i--) {
            map.put(targetIndex + i, map.get(srcIndex + i));
        }
    }

    public void put(int val, int index) {
        map.position(index);
        map.putInt(val);
    }

    public int get(int index) {
        map.position(index);
        return map.getInt();
    }

    public void putLong(long val, int index) {
        map.position(index);
        map.putLong(val);
    }

    public long getLong(int index) {
        map.position(index);
        return map.getLong();
    }

    public void putShort(short val, int index) {
        map.position(index);
        map.putShort(val);
    }

    public short getShort(int index) {
        map.position(index);
        return map.getShort();
    }

    public void close() {
        try {
            if (ch != null) {
                ch.close();
            }
        } catch (Exception e) {

        }

    }

    public void lock() {
        try {
            lock = ch.lock();
        } catch (Exception e) {
            System.out.println("lock is flying");
        }
    }

    public void release() {
        try {
            lock.release();
        } catch (Exception e) {
            System.out.println("release is flying");
        }

    }

    public int compare(int index, int length, ByteBuffer buffer) {
        map.position(index);
        int oldLimit = map.limit();
        map.limit(index+length);
        map.mark();
        int result = map.compareTo(buffer);
        map.reset();
        map.limit(oldLimit);
        return result;
    }




    public void dump(int index, int size) {

        final char[] hexArray = "0123456789ABCDEF".toCharArray();
        byte[] bytes = new byte[size];
        get(bytes, index, size);

        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        System.out.println(new String(hexChars));
    }
}
