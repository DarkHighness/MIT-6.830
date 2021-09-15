package simpledb.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private int maxSize;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);

        this.maxSize = capacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return this.size() > maxSize;
    }
}
