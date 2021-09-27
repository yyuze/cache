package org.yyuze.cache;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A basic cache.
 */
public abstract class BasicCache<K, V> implements Cacheable<K, V> {

    private final InvalidationQueue invalidQueue;

    private final ThreadPoolExecutor invalidHandlingExecutor;

    private static class InvalidEntry<K, V> {
        private final K key;
        private final V value;
        private final InvalidationHandler<K, V> handler;
        private boolean dropped;

        private InvalidEntry(K key, V value, InvalidationHandler<K, V> handler) {
            this.key = key;
            this.value = value;
            this.handler = handler;
            this.dropped = false;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            InvalidEntry<?, ?> that = (InvalidEntry<?, ?>) o;
            return Objects.equals(key, that.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key);
        }

    }

    protected BasicCache(ThreadPoolExecutor invalidHandlingExecutor) {
        this.transitionCtl = new Transition();
        this.invalidQueue = new InvalidationQueue();
        this.invalidHandlingExecutor = invalidHandlingExecutor;
        startInvalidEntryHandlingThread();
        this.invalidatingLock = new ReentrantLock();
        this.invalidating = this.invalidatingLock.newCondition();
    }

    class Transition {
        private final Set<K> unstable;

        private final Lock sync;

        private final Condition onTransition;

        private Transition() {
            this.unstable = new HashSet<>();
            this.sync = new ReentrantLock();
            this.onTransition = this.sync.newCondition();
        }

        final void markUnstable(K key) {
            this.sync.lock();
            try {
                while (this.unstable.contains(key)) {
                    CacheLogger.logger.debug("[CACHE] unstable... {}", key);
                    this.onTransition.await();
                }
                this.unstable.add(key);
                CacheLogger.logger.debug("[CACHE] mark unstable: {}", key);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                CacheLogger.logger.error("", e);
            } finally {
                this.sync.unlock();
            }
        }

        final boolean isUnstable(KeyPattern<K> pattern) {
            this.sync.lock();
            try {
                return this.unstable.stream().anyMatch(pattern::matchs);
            } finally {
                this.sync.unlock();
            }
        }

        final void markStable(K key) {
            this.sync.lock();
            try {
                this.unstable.remove(key);
                this.onTransition.signalAll();
                CacheLogger.logger.debug("[CACHE] mark stable: {}", key);
            } finally {
                this.sync.unlock();
            }
        }
    }

    protected final Transition transitionCtl;

    /**
     * start thread which handles invalidation processing.
     */
    private void startInvalidEntryHandlingThread() {
        new Thread(
                () -> {
                    CacheLogger.logger.debug("[CACHE] Invalidation handling thread initiated.");
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            InvalidEntry<K, V> entry = invalidQueue.take();
                            this.invalidHandlingExecutor.execute(() -> {
                                try {
                                    if (entry.dropped) {
                                        entry.handler.drop(entry.key, entry.value);
                                        CacheLogger.logger.debug("[CACHE] invalidation dropped: {}", entry.key);
                                    } else {
                                        entry.handler.handle(entry.key, entry.value);
                                        CacheLogger.logger.debug("[CACHE] invalidation handled: {}", entry.key);
                                    }
                                } catch (Exception e) {
                                    CacheLogger.logger.error("", e);
                                } finally {
                                    transitionCtl.markStable(entry.key);
                                    signalInvalidated();
                                }
                            });
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        CacheLogger.logger.error("", e);
                    }
                },
                String.format("Cache<%s, %s> invalidation thread", keyClassName(), valueClassName())
        ).start();
    }

    protected final boolean pushToInvalidQueue(K key, V value, InvalidationHandler<K, V> invalidationHandler) {
        return invalidQueue.add(new InvalidEntry<>(key, value, invalidationHandler));
    }

    protected final V resurrect(K key) {
        InvalidEntry<K, V> entry = this.invalidQueue.getLastMatch(key);
        V ret = entry == null ? null : entry.value;
        if (ret != null) {
            CacheLogger.logger.debug("[CACHE] resurrection iteration hit {}", key);
        }
        return ret;
    }

    /**
     * Idempotent method.
     */
    protected V invalid(K key, boolean discard, InvalidationHandler<K, V> invalidationHandler) {
        return this.remove(key, (k, v) -> {
            if (v != null) {
                if (discard) {
                    try {
                        invalidationHandler.drop(k, v);
                    } catch (Exception e) {
                        CacheLogger.logger.error("", e);
                        return v;
                    }
                } else {
                    if (!this.pushToInvalidQueue(k, v, invalidationHandler)) {
                        return v;
                    }
                }
            }
            return null;
        });
    }

    @Override
    public List<V> invalidateCache(
            boolean discard,
            KeyPattern<K> pattern,
            InvalidationHandler<K, V> invalidationHandler
    ) {
        if (discard) {
            this.invalidQueue.discard(pattern);
        }
        return this.keys()
                   .stream()
                   .filter(pattern::matchs)
                   .map(k -> invalid(k, discard, invalidationHandler))
                   .filter(Objects::nonNull)
                   .collect(Collectors.toList());
    }

    @Override
    public V getCache(K key, CacheValueBuilder<K, V> builder) throws Exception {
        CacheLogger.logger.debug("[CACHE] get: {}", key);
        transitionCtl.markUnstable(key);
        try {
            return this.get(key, (k, v) -> {
                if (v == null) {
                    v = this.resurrect(k);
                    try {
                        v = v == null ? builder.build(key) : builder.resurrect(k, v);
                    } catch (Exception e) {
                        CacheLogger.logger.error("[CACHE] get operation error: ", e);
                        v = null;
                    }
                }
                return v;
            });
        } finally {
            transitionCtl.markStable(key);
        }
    }

    private final Lock invalidatingLock;
    private final Condition invalidating;

    @Override
    public void wait4Invalidating(KeyPattern<K> pattern) throws InterruptedException {
        invalidatingLock.lock();
        try {
            while (this.invalidQueue.refers(pattern) || this.transitionCtl.isUnstable(pattern)) {
                invalidating.await();
            }
        } finally {
            invalidatingLock.unlock();
        }
    }

    private void signalInvalidated() {
        invalidatingLock.lock();
        try {
            invalidating.signalAll();
        } finally {
            invalidatingLock.unlock();
        }
    }

    /**
     * Atomically get value corresponding to @key using @operation.
     */
    protected abstract V get(K key, BiFunction<K, V, V> operation);

    /**
     * Atomically remove value corresponding to @key using @operation.
     */
    protected abstract V remove(K key, BiFunction<K, V, V> operation);

    /**
     * Get all cached keys.
     */
    protected abstract List<K> keys();

    /**
     * Get class name of key.
     */
    protected abstract String keyClassName();

    /**
     * Get class name of value.
     */
    protected abstract String valueClassName();

    static class Node<K, V> {
        InvalidEntry<K, V> item;
        Node<K, V> next;

        Node(InvalidEntry<K, V> x) {
            item = x;
        }
    }

    private class InvalidationQueue {
        private final int capacity;
        private final AtomicInteger count = new AtomicInteger();
        private Node<K, V> head;
        private Node<K, V> last;
        private final ReentrantLock takeLock = new ReentrantLock();
        private final Condition notEmpty = takeLock.newCondition();
        private final ReentrantLock putLock = new ReentrantLock();
        private final Condition notFull = putLock.newCondition();

        /**
         * Trace the number of a specified key's appearance in the current invalidation queue.
         * Used to identify whether a key-value pair should be handled or not when the key is
         * invalidated:
         * 1. When the ref-num is bigger than one, there are caches of later version in the
         * queue so that the entry should be dropped.
         * 2. When the ref-num is equal to one, the cache is unique and latest in the queue,
         * of which entry should be handled.
         */
        private final ConcurrentHashMap<K, Long> refNum;

        /**
         * enqueue and dequeue are not mutual exclusive.
         */
        private final Lock refNumLock;

        private final ReentrantReadWriteLock elementLock;

        public InvalidationQueue() {
            this(Integer.MAX_VALUE);
        }

        public InvalidationQueue(int capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException();
            }
            this.capacity = capacity;
            last = head = new Node<>(null);
            this.refNum = new ConcurrentHashMap<>();
            this.refNumLock = new ReentrantLock();
            this.elementLock = new ReentrantReadWriteLock();
        }

        private void signalNotEmpty() {
            takeLock.lock();
            try {
                notEmpty.signal();
            } finally {
                takeLock.unlock();
            }
        }

        private void signalNotFull() {
            putLock.lock();
            try {
                notFull.signal();
            } finally {
                putLock.unlock();
            }
        }

        private void enqueue(Node<K, V> node) {
            this.elementLock.writeLock().lock();
            try {
                last = last.next = node;
                refNumLock.lock();
                try {
                    K refKey = node.item.key;
                    refNum.merge(refKey, 1L, Long::sum);
                    CacheLogger.logger.debug(
                            "[CACHE] invalidation enqueue: {}, queue ref: {}", refKey, refNum.get(refKey)
                    );
                } finally {
                    refNumLock.unlock();
                }
            } finally {
                this.elementLock.writeLock().unlock();
            }
        }

        private InvalidEntry<K, V> dequeue() {
            this.elementLock.writeLock().lock();
            try {
                Node<K, V> h = head;
                Node<K, V> first = h.next;
                h.next = h; // help GC
                head = first;
                InvalidEntry<K, V> x = first.item;
                first.item = null;
                refNumLock.lock();
                try {
                    K refKey = x.key;
                    Long num = this.refNum.remove(refKey);
                    x.dropped = num != 1;
                    if (x.dropped) {
                        this.refNum.put(refKey, --num);
                    }
                    CacheLogger.logger.debug(
                            "[CACHE] invalidation dequeue: {}, queue ref: {}",
                            refKey, this.refNum.getOrDefault(refKey, 0L)
                    );
                } finally {
                    refNumLock.unlock();
                }
                return x;
            } finally {
                this.elementLock.writeLock().unlock();
            }
        }

        public boolean add(InvalidEntry<K, V> e) {
            if (e == null) {
                throw new NullPointerException();
            }
            if (count.get() == capacity) {
                return false;
            }
            int c = -1;
            Node<K, V> node = new Node<>(e);
            putLock.lock();
            try {
                if (count.get() < capacity) {
                    enqueue(node);
                    c = count.getAndIncrement();
                    if (c + 1 < capacity) {
                        notFull.signal();
                    }
                }
            } finally {
                putLock.unlock();
            }
            if (c == 0) {
                signalNotEmpty();
            }
            return c >= 0;
        }

        public InvalidEntry<K, V> take() throws InterruptedException {
            InvalidEntry<K, V> x;
            int c;
            takeLock.lockInterruptibly();
            try {
                while (count.get() == 0) {
                    notEmpty.await();
                }
                transitionCtl.markUnstable(head.next.item.key);
                x = dequeue();
                c = count.getAndDecrement();
                if (c > 1) {
                    notEmpty.signal();
                }
            } finally {
                takeLock.unlock();
            }
            if (c == capacity) {
                signalNotFull();
            }
            return x;
        }

        public InvalidEntry<K, V> getLastMatch(K key) {
            this.elementLock.readLock().lock();
            try {
                InvalidEntry<K, V> match = null;
                Node<K, V> i = this.head;
                while (i != null) {
                    if (i.item != null && i.item.key.equals(key) && !i.item.dropped) {
                        match = i.item;
                    }
                    i = i.next;
                }
                return match;
            } finally {
                this.elementLock.readLock().unlock();
            }
        }

        public void discard(KeyPattern<K> pattern) {
            this.elementLock.readLock().lock();
            try {
                Node<K, V> i = this.head;
                while (i != null) {
                    if (i.item != null && pattern.matchs(i.item.key)) {
                        i.item.dropped = true;
                        CacheLogger.logger.debug("[CACHE] discard from invalidation queue: {}", i.item.key);
                    }
                    i = i.next;
                }
            } finally {
                this.elementLock.readLock().unlock();
            }
        }

        public boolean refers(KeyPattern<K> pattern) {
            this.refNumLock.lock();
            try {
                return this.refNum.keySet().stream().anyMatch(pattern::matchs);
            } finally {
                this.refNumLock.unlock();
            }
        }
    }
}
