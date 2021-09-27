package org.yyuze.cache.impl;

import org.yyuze.cache.LifecycleCache;
import org.yyuze.cache.MemoryMap;
import org.yyuze.cache.kv.Key;
import org.yyuze.cache.kv.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

public class LifeCycleTestCache extends LifecycleCache<Key, Value> {

    private final MemoryMap<Key, Value> dataTable;

    public LifeCycleTestCache() {
        super(
                new ThreadPoolExecutor(
                        15,
                        15,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new SynchronousQueue<>(),
                        (r, executor) -> {
                            try {
                                executor.getQueue().put(r);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                )
        );
        dataTable = new MemoryMap<>();
    }

    @Override
    protected Value get(Key key, BiFunction<Key, Value, Value> operation) {
        return dataTable.atomicGetAndRemapping(key, operation);
    }

    @Override
    protected Value remove(Key key, BiFunction<Key, Value, Value> operation) {
        return dataTable.atomicRemove(key, operation);
    }

    @Override
    protected List<Key> keys() {
        return dataTable == null ? new ArrayList<>() : new ArrayList<>(dataTable.keySet());
    }

    @Override
    protected String keyClassName() {
        return Key.class.getSimpleName();
    }

    @Override
    protected String valueClassName() {
        return Value.class.getSimpleName();
    }

    private static final InvalidationHandler<Key, Value> defaultHandler = new InvalidationHandler<Key, Value>() {
        @Override
        public void handle(Key key, Value value) throws Exception {
            Thread.sleep(10);
        }

        @Override
        public void drop(Key key, Value value) {
            // do nothing
        }
    };

    @Override
    public InvalidationHandler<Key, Value> getDefaultInvalidationHandler() {
        return defaultHandler;
    }

    @Override
    protected long capacity() {
        return 25000;
    }

    @Override
    protected long lifecycle() {
        return 20;
    }
}