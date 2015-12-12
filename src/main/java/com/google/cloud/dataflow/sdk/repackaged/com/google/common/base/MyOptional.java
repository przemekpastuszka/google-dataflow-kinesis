package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;

/**
 * Created by ppastuszka on 12.12.15.
 */
public abstract class MyOptional<T> extends Optional<T> {
    public static <T> Optional<T> absent() {
        return AbsentWithNoSuchElementException.withType();
    }
}
