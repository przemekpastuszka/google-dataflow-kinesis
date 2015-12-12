package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;


import com.google.cloud.dataflow.sdk.repackaged.com.google.common.annotations.GwtCompatible;

import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.Set;

@GwtCompatible
public class AbsentWithNoSuchElementException<T> extends Optional<T> {
    private final static AbsentWithNoSuchElementException INSTANCE = new AbsentWithNoSuchElementException();
    private static final long serialVersionUID = 0L;

    public static <T> Optional<T> withType() {
        return INSTANCE;
    }

    @Override
    public boolean isPresent() {
        return Optional.<T>absent().isPresent();
    }

    @Override
    public T get() {
        throw new NoSuchElementException();
    }

    @Override
    public T or(T t) {
        return Optional.<T>absent().or(t);
    }

    @Override
    public Optional<T> or(Optional<? extends T> optional) {
        return Optional.<T>absent().or(optional);
    }

    @Override
    public T or(Supplier<? extends T> supplier) {
        return Optional.<T>absent().or(supplier);
    }

    @Nullable
    @Override
    public T orNull() {
        return Optional.<T>absent().orNull();
    }

    @Override
    public Set<T> asSet() {
        return Optional.<T>absent().asSet();
    }

    @Override
    public <V> Optional<V> transform(Function<? super T, V> function) {
        return Optional.<T>absent().transform(function);
    }

    @Override
    public boolean equals(@Nullable Object o) {
        return Optional.<T>absent().equals(o);
    }

    @Override
    public int hashCode() {
        return Optional.<T>absent().hashCode();
    }

    @Override
    public String toString() {
        return Optional.<T>absent().toString();
    }
}
