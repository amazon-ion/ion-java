// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IteratorSystemProcessingTest.class,
    TextByteArrayIteratorSystemProcessingTest.class,
    BinaryByteArrayIteratorSystemProcessingTest.class,
    JavaReaderIteratorSystemProcessingTest.class,
    BinaryStreamIteratorSystemProcessingTest.class,
    TextStreamIteratorSystemProcessingTest.class,
    LoadTextBytesSystemProcessingTest.class,
    LoadTextStreamSystemProcessingTest.class,
    LoadBinaryBytesSystemProcessingTest.class,
    LoadBinaryStreamSystemProcessingTest.class,
    DatagramBytesSystemProcessingTest.class,
    DatagramIteratorSystemProcessingTest.class,
    BinaryReaderSystemProcessingTest.class,
    DatagramTreeReaderSystemProcessingTest.class,
    TextReaderSystemProcessingTest.class,
    NewDatagramIteratorSystemProcessingTest.class,
    TrBwBrProcessingTest.class
})
public class SystemProcessingTests
{
}
