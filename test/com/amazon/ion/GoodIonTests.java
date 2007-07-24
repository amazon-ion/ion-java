/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;

import junit.framework.TestSuite;


public class GoodIonTests
    extends DirectoryTestSuite
{
    private static class GoodIonTextTest
        extends FileTestCase
    {
        public GoodIonTextTest(File ionText)
        {
            super(ionText);
        }

        public void runTest()
            throws Exception
        {
            readIonText(myTestFile);
        }
    }


    public static TestSuite suite()
    {
        return new GoodIonTests();
    }


    public GoodIonTests()
    {
        super("good");
    }


    @Override
    protected GoodIonTextTest makeTest(File ionFile)
    {
        return new GoodIonTextTest(ionFile);
    }
}
