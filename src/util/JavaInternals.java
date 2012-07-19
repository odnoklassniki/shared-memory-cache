package util;

import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class JavaInternals {
    private static final Unsafe unsafe;

    static {
        try {
            unsafe = (Unsafe) getField(Unsafe.class, "theUnsafe").get(null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Field getField(Class cls, String name) {
        try {
            Field f = cls.getDeclaredField(name);
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Method getMethod(Class cls, String name, Class... params) {
        try {
            Method m = cls.getDeclaredMethod(name, params);
            m.setAccessible(true);
            return m;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static Unsafe getUnsafe() {
        return unsafe;
    }

    public static long allocateMemory(long size, Object holder) {
        final long address = unsafe.allocateMemory(size);
        Cleaner.create(holder, new Runnable() {
            @Override
            public void run() {
                unsafe.freeMemory(address);
            }
        });
        return address;
    }

    public static long getByteBufferAddress(ByteBuffer buffer) {
        try {
            return getField(Buffer.class, "address").getLong(buffer);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }
}
