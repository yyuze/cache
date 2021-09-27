package org.yyuze.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yyuze.cache.impl.FixedSizeTestCache;
import org.yyuze.cache.kv.Key;
import org.yyuze.cache.kv.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class FixedSizeCacheTest {

    private static final Cacheable.CacheValueBuilder<Key, Value> builder = new Cacheable.CacheValueBuilder<Key, Value>() {
        @Override
        public Value build(Key key) {
            return new Value();
        }

        @Override
        public Value resurrect(Key reKey, Value reValue) {
            return reValue;
        }
    };

    @Test
    void concurrencyTest() throws InterruptedException {
        FixedSizeTestCache cache = new FixedSizeTestCache();
        Lock sync = new ReentrantLock();
        Stack<Key> everCached = new Stack<>();
        List<Thread> threads = new ArrayList<>();
        Runnable task = () -> {
            for (int i = 0; i < 20000; ++i) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                Key key = new Key(i % 20);
                sync.lock();
                everCached.push(key);
                sync.unlock();
                try {
                    cache.getCache(key, builder);
                } catch (Exception e) {
                    threads.forEach(Thread::interrupt);
                    Assertions.fail(e);
                }
            }
        };
        for(int i = 0; i < 5; ++i) {
            threads.add(new Thread(task, "t-" + i));
        }
        threads.forEach(Thread::start);
        for (Thread thread : threads) {
            thread.join();
        }
        while (!cache.invalidateCache(false, everCached::contains).isEmpty()) {
            Thread.sleep(100L);
        }
    }
}