// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 *
 */
public class IonWriterTests
{
    public static Test suite()
    {
        TestSuite suite = new TestSuite("IonWriterTests");

        suite.addTestSuite(TextWriterTest.class);
        suite.addTestSuite(BinaryWriterTest.class);
        suite.addTestSuite(ValueWriterTest.class);

        return suite;
    }
}
