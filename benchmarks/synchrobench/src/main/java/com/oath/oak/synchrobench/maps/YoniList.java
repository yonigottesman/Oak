package com.oath.oak.synchrobench.maps;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class YoniList<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private ConcurrentSkipListMap<Object, ByteBuffer> skipListMap;
    private OakMemoryAllocator allocator;


    Comparator<Object> comparator;
    public YoniList() {

        comparator= (o1, o2) ->
        {
            if (o1 instanceof MyBuffer) {
                return -1*MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) o2,(MyBuffer) o1);
            } else if (o2 instanceof MyBuffer) {
                return MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) o1, (MyBuffer)o2);
            } else {
                return MyBufferOak.keysComparator.compareSerializedKeys((ByteBuffer)o1,(ByteBuffer)o2);
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new OakNativeMemoryAllocator(Integer.MAX_VALUE);
    }

    @Override
    public boolean getOak(K key) {
        return skipListMap.get(key) != null;
    }

    @Override
    public void putOak(K key, V value) {

        ByteBuffer ret = skipListMap.computeIfPresent(key, (k, v) -> {
            synchronized (v) {
                MyBufferOak.serializer.serialize(value, v);
                return v;
            }
        });
        if (ret == null) {
            ByteBuffer keybb = allocator.allocate(MyBufferOak.serializer.calculateSize(key));
            ByteBuffer valuebb = allocator.allocate(MyBufferOak.serializer.calculateSize( value));
            MyBufferOak.serializer.serialize(key, keybb);
            MyBufferOak.serializer.serialize(value, valuebb);
            ByteBuffer oldValue = skipListMap.put(keybb, valuebb);
            if (oldValue != null) {
                allocator.free(oldValue);
                allocator.free(keybb);
            }
        }
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        if (skipListMap.containsKey(key)) {
            return false;
        }
        ByteBuffer keybb = allocator.allocate(MyBufferOak.serializer.calculateSize(key));
        ByteBuffer valuebb = allocator.allocate(MyBufferOak.serializer.calculateSize(value));
        MyBufferOak.serializer.serialize(key, keybb);
        MyBufferOak.serializer.serialize(value, valuebb);
        boolean ret = skipListMap.putIfAbsent(keybb, valuebb) == null;
        if (!ret) {
            allocator.free(keybb);
            allocator.free(valuebb);
        }
        return ret;
    }

    @Override
    public void removeOak(K key) {
        ByteBuffer val = skipListMap.remove(key);
        allocator.free(val);
        //TODO YONIGO - free key

    }

    @Override
    public boolean computeIfPresentOak(K key) {
        return false;
    }

    @Override
    public void computeOak(K key) {

    }

    @Override
    public boolean ascendOak(K from, int length) {
        Iterator iter = skipListMap.tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public boolean descendOak(K from, int length) {
        Iterator iter = skipListMap.descendingMap().tailMap(from, true).keySet().iterator();
        int i = 0;
        while (iter.hasNext() && i < length) {
            i++;
            iter.next();
        }
        return i == length;
    }

    @Override
    public void clear() {
        skipListMap.entrySet().forEach(entry -> {
            allocator.free(entry.getValue());
            allocator.free((ByteBuffer) entry.getKey());
        });
        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator.close();
        allocator = new OakNativeMemoryAllocator(Integer.MAX_VALUE);
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }
}
