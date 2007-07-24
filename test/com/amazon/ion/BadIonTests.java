/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;

import junit.framework.TestSuite;


public class BadIonTests
    extends DirectoryTestSuite
{
    private static class BadIonTextTest
        extends FileTestCase
    {
        public BadIonTextTest(File ionText)
        {
            super(ionText);
        }

        public void runTest()
            throws Exception
        {
            try
            {
                readIonText(myTestFile);
                fail("Expected IonException parsing "
                     + myTestFile.getAbsolutePath());
            }
            catch (IonException e) { /* good */ }
        }
    }


    public static TestSuite suite()
    {
        return new BadIonTests();
    }


    public BadIonTests()
    {
        super("bad");
    }


    @Override
    protected BadIonTextTest makeTest(File ionFile)
    {
        return new BadIonTextTest(ionFile);
    }
}
