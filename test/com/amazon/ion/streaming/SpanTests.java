// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    NonSpanReaderTest.class,
    SpanReaderTest.class,
    NonTextSpanTest.class,
    TextSpanTest.class,
    NonOffsetSpanReaderTest.class,
    OffsetSpanReaderTest.class,
    NonSeekableReaderTest.class,
    SeekableReaderTest.class
})
public class SpanTests
{
}
