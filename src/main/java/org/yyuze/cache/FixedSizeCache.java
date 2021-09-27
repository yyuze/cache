package org.yyuze.cache;


import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A cache with a fixed size place to cache entries.
 */
public abstract class FixedSizeCache<K extends FixedSizeCache.SizeMeasureable, V extends FixedSizeCache.SizeMeasureable> extends BasicCache<K, V> {

    public interface SizeMeasureable {
        long structureSize();
    }

    private final AtomicLong size;

    protected FixedSizeCache(ThreadPoolExecutor invalidHandlingExecutor) {
        super(invalidHandlingExecutor);
        size = new AtomicLong(0L);
    }

    private void decreaseSizeOf(K key, V value) {
        long dec = key.structureSize() + value.structureSize();
        long curSize = this.size.addAndGet(-dec);
        CacheLogger.logger.debug(
                "[CACHE] size decrement: {} bytes, free: {} bytes", dec, this.capacity() - curSize
        );
    }

    @Override
    protected V invalid(K key, boolean discard, InvalidationHandler<K, V> invalidationHandler) {
        return this.remove(key, (k, v) -> {
            if (v != null) {
                if (discard) {
                    try {
                        invalidationHandler.drop(k, v);
                    } catch (Exception e) {
                        CacheLogger.logger.error("", e);
                    }
                } else {
                    this.pushToInvalidQueue(k, v, invalidationHandler);
                }
                this.untraceKey(k);
                decreaseSizeOf(k, v);
            }
            return null;
        });
    }

    @Override
    public V getCache(K key, CacheValueBuilder<K, V> builder) {
        CacheLogger.logger.debug("[CACHE] get: {}", key);
        transitionCtl.markUnstable(key);
        try {
            return this.get(key, (k, v) -> {
                if (v == null) {
                    v = this.resurrect(k);
                    try {
                        v = v == null ? builder.build(k) : builder.resurrect(k, v);
                        this.size.addAndGet(v.structureSize() + k.structureSize());
                    } catch (Exception e) {
                        CacheLogger.logger.error("[CACHE] get operation error", e);
                        v = null;
                    }
                } else {
                    this.untraceKey(k);
                }
                if (v != null) {
                    this.traceKey(k);
                }
                return v;
            });
        } finally {
            transitionCtl.markStable(key);
            // one in one out, a good size-keeper policy
            if (this.size.get() > capacity()) {
                this.invalid(this.keyToSqueezeOut(), false, getDefaultInvalidationHandler());
            }
        }
    }

    /**
     * get the capacity of the cache.
     */
    protected abstract long capacity();

    /**
     * get the key to squeeze out when cache is full.
     */
    protected abstract K keyToSqueezeOut() throws NullPointerException;

    /**
     * trace the size measurable key.
     */
    protected abstract void traceKey(K key);

    /**
     * untrace the size measurable key.
     */
    protected abstract void untraceKey(K key);

}
