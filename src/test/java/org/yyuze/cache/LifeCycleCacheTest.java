package org.yyuze.cache;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.yyuze.cache.impl.LifeCycleTestCache;
import org.yyuze.cache.kv.Key;
import org.yyuze.cache.kv.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class LifeCycleCacheTest {

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
        Cacheable<Key, Value> cache = new LifeCycleTestCache();
        Lock sync = new ReentrantLock();
        Stack<Key> everCached = new Stack<>();
        List<Thread> threads = new ArrayList<>();
        Runnable task = () -> {
            for (int i = 0; i < 25000; i++) {
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                Key k = new Key(new Random().nextInt() % 200);
                sync.lock();
                everCached.push(k);
                sync.unlock();
                try {
                    System.out.println(cache.getCache(k, builder));
                } catch (Exception e) {
                    threads.forEach(Thread::interrupt);
                    Assertions.fail(e);
                }
            }
        };
        for (int i = 0; i < 16; ++i) {
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
