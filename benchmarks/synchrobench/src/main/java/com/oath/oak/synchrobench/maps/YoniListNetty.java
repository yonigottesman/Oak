package com.oath.oak.synchrobench.maps;


import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;


import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class YoniListNetty<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private ConcurrentSkipListMap<Object, ByteBuf> skipListMap;

    PooledByteBufAllocator nettyAllocator = PooledByteBufAllocator.DEFAULT;
    Comparator<Object> comparator;


//    private int compare(ByteBuffer buffer1, int base1, int len1, ByteBuffer buffer2, int base2, int len2) {
//        int n = Math.min(len1, len2);
//        for (int i = 0; i < n; i += Integer.BYTES) {
//            int cmp = Integer.compare(buffer1.getInt(base1 + i), buffer2.getInt(base2 + i));
//            if (cmp != 0) return cmp;
//        }
//        return (len1 - len2);
//    }


    public int compareSerializedKeyAndKey(ByteBuf serializedKey, MyBuffer key) {
        int cap1 = serializedKey.getInt(0);
        int pos2 = key.buffer.position();
        int cap2 = key.buffer.capacity();

        int n = Math.min(cap1, cap2);
        for (int i = 0; i < n; i += Integer.BYTES) {
            int cmp = Integer.compare(serializedKey.getInt(i + Integer.BYTES), key.buffer.getInt(pos2 +  i));
            if (cmp != 0) return cmp;
        }
        return (cap1 - cap2);

//        return compare(serializedKey, pos1 + Integer.BYTES, cap1, key.buffer, pos2, cap2);

    }


    public YoniListNetty() {

        comparator= (o1, o2) ->
        {
            if (o1 instanceof MyBuffer) {
                return -1;//*MyBufferOak.keysComparator.compareSerializedKeyAndKey(((ByteBuf) o2),(MyBuffer) o1);
            } else if (o2 instanceof MyBuffer) {
                return MyBufferOak.keysComparator.compareSerializedKeyAndKey(((ByteBuf) o1).nioBuffer(0, ((ByteBuf) o1).capacity()), (MyBuffer)o2);
            } else {
                return MyBufferOak.keysComparator.compareSerializedKeys(((ByteBuf) o1).nioBuffer(0, ((ByteBuf) o1).capacity()), ((ByteBuf) o2).nioBuffer(0, ((ByteBuf) o2).capacity()));
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);

    }

    @Override
    public boolean getOak(K key) {
        return skipListMap.get(key) != null;
    }

    private void serialize(ByteBuf targetBuffer, MyBuffer key) {
        int cap = key.buffer.capacity();
        // write the capacity in the beginning of the buffer
        targetBuffer.writeInt(cap);
        for (int i = 0; i < cap; i += Integer.BYTES) {
            targetBuffer.writeInt(key.buffer.getInt(i));
        }
    }

    @Override
    public void putOak(K key, V value) {

        ByteBuf ret = skipListMap.computeIfPresent(key, (k, v) -> {
            synchronized (v) {
                serialize(v, value);
                return v;
            }
        });
        if (ret == null) {
            ByteBuf keybb = nettyAllocator.directBuffer(MyBufferOak.serializer.calculateSize(key));
            ByteBuf valuebb = nettyAllocator.directBuffer(MyBufferOak.serializer.calculateSize( value));
            serialize(keybb, key);
            serialize(valuebb, value);
            ByteBuf oldValue = skipListMap.put(keybb, valuebb);
            if (oldValue != null) {
                keybb.release();
                oldValue.release();
            }
        }
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        if (skipListMap.containsKey(key)) {
            return false;
        }
        ByteBuf keybb = nettyAllocator.directBuffer(MyBufferOak.serializer.calculateSize(key));
        ByteBuf valuebb = nettyAllocator.directBuffer(MyBufferOak.serializer.calculateSize( value));
        serialize(keybb, key);
        serialize(valuebb, value);
        boolean ret = skipListMap.putIfAbsent(keybb, valuebb) == null;
        if (!ret) {
            keybb.release();
            valuebb.release();
        }
        return ret;
    }

    @Override
    public void removeOak(K key) {
        ByteBuf val = skipListMap.remove(key);
        val.release();
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
            ((ByteBuf)entry.getKey()).release();
            entry.getValue().release();
        });

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        System.gc();
    }

    @Override
    public int size() {
        return skipListMap.size();
    }
}
