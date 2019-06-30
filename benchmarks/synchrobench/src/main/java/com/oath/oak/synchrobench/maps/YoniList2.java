package com.oath.oak.synchrobench.maps;

import com.oath.oak.NativeAllocator.OakNativeMemoryAllocator;
import com.oath.oak.OakMemoryAllocator;
import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import com.oath.oak.synchrobench.contention.benchmark.Parameters;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

public class YoniList2<K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    private ConcurrentSkipListMap<Object, Cell> skipListMap;
    private OakMemoryAllocator allocator;


    Comparator<Object> comparator;

    public YoniList2() {

        comparator = (o1,o2) ->
        {
            //TODO YONIGO - what if key gets dfeleted?
            if (o1 instanceof MyBuffer) {

                //o2 is a node and the key is either mybuffer or bytebuffer:
                Cell cell2 = (Cell)o2;
                Object key2 = cell2.key.get();
                if (key2 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer) o1, (MyBuffer)key2);
                } else {
                    return -1*MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) key2,(MyBuffer) o1);
                }

            } else if (o2 instanceof MyBuffer) {
                Cell cell1 = (Cell)o1;
                Object key1 = cell1.key.get();
                if (key1 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer)key1, (MyBuffer)o2);
                } else {
                    return MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) key1, (MyBuffer)o2);
                }
            } else if (o1 instanceof YoniList2.Cell && o2 instanceof YoniList2.Cell){
                Cell cell1 = (Cell)o1;
                Object key1 = cell1.key.get();
                Cell cell2 = (Cell)o2;
                Object key2 = cell2.key.get();

                if (key1 instanceof MyBuffer && key2 instanceof MyBuffer) {
                    return MyBufferOak.keysComparator.compareKeys((MyBuffer) key1, (MyBuffer)key2);
                } else if (key1 instanceof ByteBuffer && key2 instanceof ByteBuffer) {
                    return MyBufferOak.keysComparator.compareSerializedKeys((ByteBuffer)key1,(ByteBuffer)key2);
                } else if (key1 instanceof MyBuffer && key2 instanceof ByteBuffer) {
                    return -1*MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) key2,(MyBuffer) key1);
                } else {
                    return MyBufferOak.keysComparator.compareSerializedKeyAndKey((ByteBuffer) key1, (MyBuffer)key2);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        };

        skipListMap = new ConcurrentSkipListMap<>(comparator);
        allocator = new OakNativeMemoryAllocator(Integer.MAX_VALUE);
    }

    @Override
    public boolean getOak(K key) {
        Cell value = skipListMap.get(key);
        if (Parameters.zeroCopy) {
            return  value != null && value.value != null;
        } else {
            if (value != null && value.value != null) {
                MyBuffer des = MyBufferOak.serializer.deserialize(value.value.get());
                return (des != null);
            } else {
                return false;
            }
        }
    }

    @Override
    public void putOak(K key, V value) {

        Cell newCell = new Cell();
        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);

        if (prevValue == null) {
            ByteBuffer keybb = allocator.allocate(MyBufferOak.serializer.calculateSize(key));
            ByteBuffer valuebb = allocator.allocate(MyBufferOak.serializer.calculateSize( value));
            MyBufferOak.serializer.serialize(key, keybb);
            MyBufferOak.serializer.serialize(value, valuebb);
            if (!newCell.value.compareAndSet(null, valuebb)) {
                allocator.free(valuebb);
            }
            newCell.key.set(keybb);
        } else {
            if (prevValue.value.get() == null) {
                ByteBuffer valuebb = allocator.allocate(MyBufferOak.serializer.calculateSize( value));
                MyBufferOak.serializer.serialize(value, valuebb);
                if (!prevValue.value.compareAndSet(null, valuebb)) {
                    allocator.free(valuebb);
                }
            } else {
                synchronized (prevValue.value) {
                    MyBufferOak.serializer.serialize(value, prevValue.value.get());
                }
            }
        }
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        Cell newCell = new Cell();

        newCell.key.set(key);
        Cell prevValue = skipListMap.putIfAbsent(newCell, newCell);
        if (prevValue == null) {
            ByteBuffer keybb = allocator.allocate(MyBufferOak.serializer.calculateSize(key));
            ByteBuffer valuebb = allocator.allocate(MyBufferOak.serializer.calculateSize( value));
            MyBufferOak.serializer.serialize(key, keybb);
            MyBufferOak.serializer.serialize(value, valuebb);
            newCell.value.set(valuebb);
            newCell.key.set(keybb);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void removeOak(K key) {
        Cell val = skipListMap.remove(key);
        allocator.free((ByteBuffer) val.key.get());
        allocator.free(val.value.get());
        // TODO YONIGO - need some sync here!
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
        iterate(iter, length);
        int i = 0;
        while (iter.hasNext() && i < length) {
            Cell cell = (Cell) iter.next();
            //only if cell is not null value is not deleted or not set yet.
            if (cell.value.get() != null)
                i++;

        }
        return i == length;
    }

    @Override
    public boolean descendOak(K from, int length) {
        Iterator iter = skipListMap.descendingMap().tailMap(from, true).keySet().iterator();
        iterate(iter, length);
        int i = 0;
        while (iter.hasNext() && i < length) {
            Cell cell = (Cell) iter.next();
            //only if cell is not null value is not deleted or not set yet.
            if (cell.value.get() != null)
                i++;
        }
        return i == length;
    }


    private boolean iterate(Iterator iter, int length) {
        int i = 0;
        while (iter.hasNext() && i < length) {
            Cell cell = (Cell) iter.next();
            //only if cell is not null value is not deleted or not set yet.
            if (cell.value.get() != null) {
                if (Parameters.zeroCopy) {
                    MyBuffer des = MyBufferOak.serializer.deserialize(cell.value.get());
                    //YONIGO - I just do this so that hopefully jvm doesnt optimize out the deserialize
                    if (des != null) i++;
                } else {
                    i++;
                }

            }
        }
        return i == length;
    }



    @Override
    public void clear() {

        skipListMap.values().forEach(cell -> {
            allocator.free((ByteBuffer) cell.key.get());
            allocator.free(cell.value.get());
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

    public class MyReference<T>{
        T reference;

        public void set(T ref){
            this.reference =ref;
        }
        public T get(){
            return this.reference;
        }

    }

    private static class Cell {
        public final AtomicReference<Object> key;
        public final AtomicReference<ByteBuffer> value;

        public Cell(){
            key = new AtomicReference<>();
            value = new AtomicReference<>();
        }
    }
}
