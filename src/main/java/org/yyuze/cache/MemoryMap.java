package org.yyuze.cache;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MemoryMap<K, V> implements Map<K, V> {

    /**
     * Can be optimized by any thread-safe Map structure.
     */
    private final Map<K, Integer> dir;

    /**
     * Can be optimized by an List structure.
     */
    private final List<MEntry<K, V>> memory;

    private long[] freeMap;

    private final ReentrantLock freeMapLock = new ReentrantLock();

    private static class MEntry<K, V> {
        K indexedBy;
        V payload;
        private final ReentrantLock lock;

        MEntry(V payload) {
            this.payload = payload;
            this.lock = new ReentrantLock();
        }

        void lock() {
            this.lock.lock();
        }

        void unlock() {
            this.lock.unlock();
        }
    }

    public MemoryMap() {
        this(new ArrayList<>(), new ConcurrentHashMap<>());
    }

    public MemoryMap(List<MEntry<K, V>> dataArea, Map<K, Integer> dir) {
        this.memory = dataArea;
        this.dir = dir;
        this.freeMap = new long[1];
        this.appendObjects(freeMap.length * 64);
    }

    private boolean isFree(int i) {
        freeMapLock.lock();
        try {
            return (freeMap[i / 64] & (0x1L << (i % 64))) == 0;
        } finally {
            freeMapLock.unlock();
        }
    }

    private void markFree(int i) {
        freeMapLock.lock();
        try {
            freeMap[i / 64] &= (~(0x1L << (i % 64)));
        } finally {
            freeMapLock.unlock();
        }
    }

    private int allocateIndex() {
        freeMapLock.lock();
        try {
            int i = 0;
            while (true) {
                for (; i < freeMap.length; ++i) {
                    if (freeMap[i] == -1) {
                        continue;
                    }
                    for (int b = 0; b < 64; ++b) {
                        long select = 0x1L << b;
                        select &= freeMap[i];
                        if (select == 0) {
                            freeMap[i] |= (0x1L << b);
                            return i * 64 + b;
                        }
                    }
                }
                extend();
            }
        } finally {
            freeMapLock.unlock();
        }
    }

    private void extend() {
        int extendSize = Math.max(1, (int) (0.1 * this.freeMap.length));
        long[] newMap = new long[extendSize + this.freeMap.length];
        System.arraycopy(this.freeMap, 0, newMap, 0, this.freeMap.length);
        appendObjects(64 * extendSize);
        this.freeMap = newMap;
    }

    private void appendObjects(int appendSize) {
        for (int i = 0; i < appendSize; ++i) {
            this.memory.add(new MEntry<>(null));
        }
    }

    public V atomicGetAndRemapping(K key, BiFunction<K, V, V> remappingFunction) {
        int index = dir.computeIfAbsent(key, k -> allocateIndex());
        MEntry<K, V> e = this.memory.get(index);
        e.lock();
        try {
            V newV = remappingFunction.apply(key, e.payload);
            dir.computeIfPresent(key, (k, i) -> {
                if (i == index) {
                    if (newV != null) {
                        e.indexedBy = k;
                        e.payload = newV;
                    } else {
                        dir.remove(key);
                        e.indexedBy = null;
                        e.payload = null;
                        this.markFree(index);
                    }
                }
                return i;
            });
            return newV;
        } finally {
            e.unlock();
        }
    }

    public V atomicRemove(K key, BiFunction<K, V, V> remappingFunction) {
        Integer index = dir.remove(key);
        if (index == null) {
            return null;
        }
        MEntry<K, V> e = memory.get(index);
        e.lock();
        try {
            if (e.indexedBy == null) {
                return null;
            }
            V old = e.payload;
            e.indexedBy = null;
            e.payload = remappingFunction.apply(key, old);
            markFree(index);
            return old;
        } finally {
            e.unlock();
        }
    }

    @Override
    public int size() {
        return this.dir.size();
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public boolean containsKey(Object o) {
        Integer index = dir.get(o);
        if (index == null) {
            return false;
        } else {
            return !isFree(index);
        }
    }

    @Override
    public boolean containsValue(Object o) {
        for (int i = 0; i < this.memory.size(); i++) {
            if (this.memory.get(i).payload == null) {
                return false;
            }
            if (this.memory.get(i).payload.equals(o)) {
                return !this.isFree(i);
            }
        }
        return false;
    }

    @Override
    public V get(Object key) {
        Integer index = this.dir.get(key);
        if (index == null || this.isFree(index)) {
            return null;
        }
        return this.memory.get(index).payload;
    }

    @Override
    public V put(K k, V v) {
        if (this.dir.containsKey(k)) {
            int index = this.dir.get(k);
            V old = this.memory.get(index).payload;
            this.memory.get(index).payload = v;
            return old;
        } else {
            int index = this.allocateIndex();
            this.dir.put(k, index);
            this.memory.get(index).payload = v;
            return null;
        }
    }

    @Override
    public V remove(Object k) {
        Integer index = this.dir.remove(k);
        if (index == null) {
            return null;
        } else {
            this.markFree(index);
            V old = this.memory.get(index).payload;
            this.memory.get(index).payload = null;
            return old;
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
            this.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void clear() {
        Arrays.fill(this.freeMap, 0L);
        this.dir.clear();
    }

    @Override
    public Set<K> keySet() {
        return this.dir.keySet();
    }

    @Override
    public Collection<V> values() {
        return this.memory.stream()
                          .map(e -> {
                          e.lock();
                          try {
                              return dir.containsKey(e.indexedBy) ? e.payload : null;
                          } finally {
                              e.unlock();
                          }
                      })
                          .filter(Objects::nonNull)
                          .collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return this.memory.stream()
                          .map(entry -> {
                          entry.lock();
                          try {
                              if (entry.payload != null && dir.containsKey(entry.indexedBy)) {
                                  return new Entry<K, V>() {

                                      @Override
                                      public K getKey() {
                                          return entry.indexedBy;
                                      }

                                      @Override
                                      public V getValue() {
                                          return entry.payload;
                                      }

                                      @Override
                                      public V setValue(V v) {
                                          return entry.payload = v;
                                      }
                                  };
                              } else {
                                  return null;
                              }
                          } finally {
                              entry.unlock();
                          }
                      })
                          .filter(Objects::nonNull)
                          .collect(Collectors.toSet());
    }
}
