package util;

import sun.nio.ch.FileChannelImpl;

import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

public class MappedFile {
    private static Method map0 = JavaInternals.getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
    private static Method unmap0 = JavaInternals.getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);

    private long addr;
    private long size;

    public MappedFile(String name, long size) throws Exception {
        size = (size + 0xfffL) & ~0xfffL;

        RandomAccessFile f = new RandomAccessFile(name, "rw");
        FileChannel ch = null;

        try {
            f.setLength(size);
            ch = f.getChannel();
            this.addr = (Long) map0.invoke(ch, 1, 0L, size);
            this.size = size;
        } finally {
            if (ch != null) {
                ch.close();
            }
            f.close();
        }
    }

    public void close() {
        if (addr != 0) {
            try {
                unmap0.invoke(null, addr, size);
            } catch (Exception e) {
                // ignore
            }
            addr = 0;
        }
    }

    public final long getAddr() {
        return addr;
    }

    public final long getSize() {
        return size;
    }
}
