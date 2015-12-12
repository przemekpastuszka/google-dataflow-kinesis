package com.google.cloud.dataflow.sdk.repackaged.com.google.common.base;

import org.junit.Test;
import java.util.NoSuchElementException;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class MyOptionalTest {
    @Test(expected = NoSuchElementException.class)
    public void absentThrowsNoSuchElementExceptionOnGet() {
        MyOptional.absent().get();
    }
}
