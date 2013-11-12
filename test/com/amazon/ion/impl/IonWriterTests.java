// Copyright (c) 2009-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 *
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TextWriterTest.class,
    BinaryWriterTest.class,
    OptimizedBinaryWriterSymbolTableTest.class,
    OptimizedBinaryWriterLengthPatchingTest.class,
    OldBinaryWriterTest.class,
    ValueWriterTest.class
})
public class IonWriterTests
{
}
