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
        private final boolean myFileIsBinary;

        public BadIonTestCase(File ionFile, boolean binary)
        {
            super(ionFile);
            myFileIsBinary = binary;
        }

        @Override
        public void runTest()
            throws Exception
        {
            try
            {
                if (myFileIsBinary)
                {
                    readIonBinary(myTestFile);
                }
                else
                {
                    readIonText(myTestFile);
                }
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
        String fileName = ionFile.getName();
        if (fileName.endsWith(".ion"))
        {
            return new BadIonTestCase(ionFile, false);
        }
        else if (fileName.endsWith(".10n"))
        {
            return new BadIonTestCase(ionFile, true);
        }
        return null;
    }
}
