package org.yyuze.cache;

import java.util.List;

/**
 * Interfaces of cache operation.
 */
public interface Cacheable<K, V> {

    interface CacheValueBuilder<K, V> {
        V build(K key) throws Exception;

        V resurrect(K reKey, V reValue) throws Exception;
    }

    interface InvalidationHandler<K, V> {
        void handle(K key, V value) throws Exception;

        void drop(K key, V value) throws Exception;
    }

    interface KeyPattern<K> {
        boolean matchs(K key);
    }

    interface Action<K, V> {
        void act(K key, V value) throws Exception;
    }

    /**
     * invalidate keys which is matched with the pattern.
     */
    List<V> invalidateCache(boolean discard, KeyPattern<K> pattern, InvalidationHandler<K, V> invalidationHandler);

    /**
     * invalidate keys which is matched with the pattern using default handler.
     */
    default List<V> invalidateCache(boolean discard, KeyPattern<K> pattern) {
        return this.invalidateCache(discard, pattern, this.getDefaultInvalidationHandler());
    }

    default List<V> invalidateAllCache(boolean discard, InvalidationHandler<K, V> invalidationHandler) {
        return this.invalidateCache(discard, key -> true, invalidationHandler);
    }

    default List<V> invalidateAllCache(boolean discard) {
        return this.invalidateAllCache(discard, getDefaultInvalidationHandler());
    }

    /**
     * wait for invalidation processing, include handle and drop.
     */
    void wait4Invalidating(KeyPattern<K> pattern) throws InterruptedException;

    /**
     * Atomically get value corresponding to key from cache.
     *
     * @param key     key to cache
     * @param builder used to build cache if there is not contained in cache
     * @return value corresponding to key
     */
    V getCache(K key, CacheValueBuilder<K, V> builder) throws Exception;

    /**
     * Default handler.
     */
    InvalidationHandler<K, V> getDefaultInvalidationHandler();
}
