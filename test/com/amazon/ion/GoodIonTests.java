/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;
import junit.framework.TestSuite;


public class GoodIonTests
    extends DirectoryTestSuite
{
    private static class GoodIonTestCase
        extends FileTestCase
    {
        private final boolean myFileIsBinary;

        public GoodIonTestCase(File ionFile, boolean binary)
        {
            super(ionFile);
            myFileIsBinary = binary;
        }

        public void runTest()
            throws Exception
        {
            if (myFileIsBinary)
            {
                readIonBinary(myTestFile);
            }
            else
            {
                readIonText(myTestFile);
            }
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
    protected FileTestCase makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        if (fileName.endsWith(".ion"))
        {
            return new GoodIonTestCase(ionFile, false);
        }
        else if (fileName.endsWith(".10n"))
        {
            return new GoodIonTestCase(ionFile, true);
        }
        return null;
    }
}
