package org.yyuze.cache.kv;

import org.yyuze.cache.LifecycleCache;

import java.util.Objects;

public class Key implements LifecycleCache.LifecycleTraceable {

    public final int id;

    public Key(int id) {
        this.id = id;
    }

    @Override
    public long structureSize() {
        return 500L;
    }

    @Override
    public long expiredAt() {
        return expiredAt;
    }

    private long expiredAt;

    @Override
    public void setExpiration(long timestamp) {
        this.expiredAt = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Key key = (Key) o;
        return id == key.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "" + id;
    }
}