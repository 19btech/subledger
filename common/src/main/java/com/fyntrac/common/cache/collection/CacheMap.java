package com.fyntrac.common.cache.collection;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CacheMap<T> implements Serializable {
    @Serial
    private static final long serialVersionUID = 5784726522766687464L;
    private Map<String, T> map;

    // Constructor
    public CacheMap() {
        this.map = new HashMap<>();
    }

    // Method to add an entry to the map
    public void put(String mapKey, T value) {
        map.put(mapKey, value);
    }

    // Method to get the map based on the key
    public Map<String, T> getMap() {
        return map;
    }

    // Method to get a value from the map based on a specific key
    public T getValue(String mapKey) {
        return map.get(mapKey);
    }
}