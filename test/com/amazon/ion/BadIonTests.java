/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;
import junit.framework.TestSuite;


public class BadIonTests
    extends DirectoryTestSuite
{
    private static class BadIonTestCase
        extends FileTestCase
    {
        public BadIonTestCase(File ionFile)
        {
            super(ionFile);
        }

        @Override
        public void runTest()
            throws Exception
        {
            try
            {
                load(myTestFile);
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
    protected BadIonTestCase makeTest(File ionFile)
    {
        return new BadIonTestCase(ionFile);
    }
}
