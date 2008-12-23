/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 */
public class SystemProcessingTests
{
    public static Test suite()
    {
        TestSuite suite =
            new TestSuite("SystemProcessingTests");

        suite.addTestSuite(IteratorSystemProcessingTest.class);
        suite.addTestSuite(DatagramBytesSystemProcessingTest.class);
        suite.addTestSuite(DatagramIteratorSystemProcessingTest.class);
        suite.addTestSuite(BinaryReaderSystemProcessingTest.class);
        suite.addTestSuite(DatagramTreeReaderSystemProcessingTest.class);
        suite.addTestSuite(TextReaderSystemProcessingTest.class);
        suite.addTestSuite(NewDatagramIteratorSystemProcessingTest.class);

        return suite;
    }
}
