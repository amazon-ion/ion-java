// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ReaderOffsetSpanTest.class,
    NonSpanReaderTest.class,
    CurrentSpanTest.class,
    NonTextSpanTest.class,
    TextSpanTest.class,
    NonHoistingReaderTest.class,
    SpanHoistingTest.class,
})
public class SpanTests
{
}
