package com.oath.oak.synchrobench.maps;


import com.oath.oak.synchrobench.contention.abstractions.CompositionalOakMap;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;



public class MapDb <K extends MyBuffer, V extends MyBuffer> implements CompositionalOakMap<K, V> {
    BTreeMap<byte[], byte[]> map;
    public MapDb() {
        DB db = DBMaker.memoryDirectDB().make();
        map = db.treeMap("map")
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
                .create();
    }

    @Override
    public boolean getOak(K key) {
        return map.get(key.buffer.array()) != null;
    }

    @Override
    public void putOak(K key, V value) {
        map.put(key.buffer.array(), value.buffer.array());
    }

    @Override
    public boolean putIfAbsentOak(K key, V value) {
        return map.putIfAbsentBoolean(key.buffer.array(), value.buffer.array());
    }

    @Override
    public void removeOak(K key) {

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
        return false;
    }

    @Override
    public boolean descendOak(K from, int length) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public int size() {
        return 0;
    }
}
