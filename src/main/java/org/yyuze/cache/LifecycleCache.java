package org.yyuze.cache;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A cache whose cached entries have a lifecycle.
 */
public abstract class LifecycleCache
        <K extends LifecycleCache.LifecycleTraceable, V extends FixedSizeCache.SizeMeasureable> extends FixedSizeCache<K, V> {

    public interface LifecycleTraceable extends SizeMeasureable {
        long expiredAt();

        void setExpiration(long timestamp);
    }

    protected LifecycleCache(ThreadPoolExecutor invalidHandlingExecutor) {
        super(invalidHandlingExecutor);
        startExpirationPollingThread();
    }

    /**
     * start thread which detects cache entries out of working set.
     */
    private void startExpirationPollingThread() {
        new Thread(
                () -> {
                    CacheLogger.logger.debug("[CACHE] Expiration polling thread initiated.");
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Thread.sleep(this.lifecycle() / 10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        try {
                            this.invalidateCache(false, k -> k.expiredAt() <= System.currentTimeMillis());
                        } catch (Exception e) {
                            CacheLogger.logger.error("unexpected error", e);
                        }
                    }
                },
                String.format("Cache<%s, %s> ttl polling Thread", keyClassName(), valueClassName())
        ).start();
    }

    /**
     * a PQ sorted by expiration date for implementing WS algorithm.
     */
    private final PriorityBlockingQueue<K> workingSet = new PriorityBlockingQueue<>(
            11,
            Comparator.comparingLong(LifecycleTraceable::expiredAt)
    );


    @Override
    protected K keyToSqueezeOut() {
        return workingSet.peek();
    }

    @Override
    protected void untraceKey(K key) throws NullPointerException {
        if (!workingSet.remove(key)) {
            throw new NullPointerException(String.format("%s is not traced", key));
        }
    }

    protected void traceKey(K key) {
        key.setExpiration(System.currentTimeMillis() + lifecycle());
        workingSet.put(key);
    }

    /**
     * get timeout limit.
     */
    protected abstract long lifecycle();

}
