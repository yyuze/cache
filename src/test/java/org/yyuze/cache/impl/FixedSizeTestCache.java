package org.yyuze.cache.impl;

import org.yyuze.cache.FixedSizeCache;
import org.yyuze.cache.MemoryMap;
import org.yyuze.cache.kv.Key;
import org.yyuze.cache.kv.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

public class FixedSizeTestCache extends FixedSizeCache<Key, Value> {

    private final MemoryMap<Key, Value> dataTable;

    private final Lock fifoLock = new ReentrantLock();

    private final ArrayList<Key> fifo;

    public FixedSizeTestCache() {
        super(
                new ThreadPoolExecutor(
                        3,
                        3,
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
        this.dataTable = new MemoryMap<>();
        this.fifo = new ArrayList<>();
    }

    @Override
    protected Value get(Key key, BiFunction<Key, Value, Value> operation) {
        return this.dataTable.atomicGetAndRemapping(key, operation);
    }

    @Override
    protected Value remove(Key key, BiFunction<Key, Value, Value> operation) {
        return this.dataTable.atomicRemove(key, operation);
    }

    @Override
    protected List<Key> keys() {
        return new ArrayList<>(this.dataTable.keySet());
    }

    @Override
    protected String keyClassName() {
        return Key.class.getSimpleName();
    }

    @Override
    protected String valueClassName() {
        return Value.class.getSimpleName();
    }

    @Override
    public InvalidationHandler<Key, Value> getDefaultInvalidationHandler() {
        return new InvalidationHandler<Key, Value>() {
            @Override
            public void handle(Key key, Value value) {
            }

            @Override
            public void drop(Key key, Value value) {
            }
        };
    }

    @Override
    protected long capacity() {
        return 25000L;
    }

    @Override
    protected Key keyToSqueezeOut() throws NullPointerException {
        fifoLock.lock();
        try {
            return fifo.isEmpty() ? null : fifo.get(0);
        } finally {
            fifoLock.unlock();
        }
    }

    @Override
    protected void untraceKey(Key key) throws NullPointerException {
        fifoLock.lock();
        try {
            if (!this.fifo.remove(key)) {
                throw new NullPointerException(String.format("%s is not traced", key));
            }
        } finally {
            fifoLock.unlock();
        }
    }

    @Override
    protected void traceKey(Key key) throws NullPointerException {
        fifoLock.lock();
        try {
            this.fifo.remove(key);
            this.fifo.add(key);
        } finally {
            fifoLock.unlock();
        }
    }
}
