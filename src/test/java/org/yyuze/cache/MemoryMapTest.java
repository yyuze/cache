package org.yyuze.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yyuze.cache.kv.Key;
import org.yyuze.cache.kv.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class MemoryMapTest {

    private static MemoryMap<Key, Value> map;

    private static List<Value> values;

    @BeforeEach
    void init() {
        map = new MemoryMap<>();
        values = new ArrayList<>();
        // put test
        for (int i = 0; i < 10; ++i) {
            Value v = new Value();
            Key k = new Key(i);
            map.put(k, v);
            values.add(v);
        }
    }

    @Test
    void getTest() {
        for (int i = 0; i < 10; ++i) {
            assertEquals(values.get(i), map.get(new Key(i)));
        }
    }

    @Test
    void putTest() {
        for (int i = 0; i < 10; ++i) {
            Value v = new Value();
            values.remove(i);
            values.add(i, v);
            map.put(new Key(i), v);
        }
        for (int i = 0; i < 10; ++i) {
            assertEquals(values.get(i), map.get(new Key(i)));
        }
    }

    @Test
    void containsKeyTest() {
        assertTrue(map.containsKey(new Key(3)));
        assertFalse(map.containsKey(new Key(123)));
    }

    @Test
    void containsValueTest() {
        assertTrue(map.containsValue(values.get(3)));
        assertFalse(map.containsValue(new Value()));
    }

    @Test
    void removeTest() {
        for (int i = 0; i < 100; ++i) {
            int rmIndex = Math.abs(new Random(i).nextInt()) % 10;
            Key rmK = new Key(rmIndex);
            Value testV = map.remove(rmK);
            assertFalse(map.containsKey(rmK));
            if (testV != null) {
                assertEquals(testV, values.get(rmIndex));
            }
        }
    }

    @Test
    void extendTest() {
        List<Value> values = new ArrayList<>();
        for (int i = 0; i < 101; ++i) {
            Value v = new Value();
            map.put(new Key(i), v);
            values.add(v);
        }
        for (int i = 0; i < 101; ++i) {
            assertEquals(values.get(i), map.get(new Key(i)));
        }
    }

    @Test
    void modifyTest() {
        Key k = new Key(1);
        Value v = map.get(k);
        String newData = "this is new data";
        v.data = newData;
        assertEquals(newData, map.get(k).data);
    }

    private List<Key> randSelectFrom(Map<Key, Value> set, int mod) {
        return set.keySet()
                  .stream()
                  .map(k -> k.id % mod == 0
                          ? null
                          : k
                  )
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList());
    }

    /**
     * Using threads with different operation (add/remove) to access the MemoryMap
     * with an external tracer which traces what key-values should be in the MemoryMap.
     */
    @Test
    void consistencyTest() throws InterruptedException {
        MemoryMap<Key, Value> mm = new MemoryMap<>();
        Map<Key, Value> tracer = new ConcurrentHashMap<>();
        Runnable addTask = () -> {
            for (int i = 0; i < 100000; ++i) {
                mm.atomicGetAndRemapping(
                        new Key(new Random().nextInt()),
                        (k, v) -> {
                            Value newV;
                            if (v != null) {
                                newV = v;
                            } else {
                                newV = new Value();
                                tracer.put(k, newV);
                            }
                            return newV;
                        }
                );
            }
        };
        Runnable removeTask = () ->
                randSelectFrom(tracer, new Random().nextInt(100) % 13 + 13).forEach(
                        key -> mm.atomicRemove(key, (k, v) -> {
                            if (v != null) {
                                assertEquals(tracer.remove(k), v);
                            }
                            return null;
                        })
                );
        List<Thread> addThreads = new ArrayList<>();
        List<Thread> removeThreads = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            addThreads.add(new Thread(addTask));
        }
        for (int i = 0; i < 3; ++i) {
            removeThreads.add(new Thread(removeTask));
        }
        addThreads.forEach(Thread::start);
        removeThreads.forEach(Thread::start);
        for (Thread addThread : addThreads) {
            addThread.join();
        }
        for (Thread removeThread : removeThreads) {
            removeThread.join();
        }
    }
}
