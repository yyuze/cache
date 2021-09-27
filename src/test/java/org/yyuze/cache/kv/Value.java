package org.yyuze.cache.kv;


import org.yyuze.cache.FixedSizeCache;

import java.util.Objects;
import java.util.UUID;

public class Value implements FixedSizeCache.SizeMeasureable {

    public String data = UUID.randomUUID().toString();

    @Override
    public long structureSize() {
        return 600L;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Value value = (Value) o;
        return Objects.equals(data, value.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return this.data;
    }
}
