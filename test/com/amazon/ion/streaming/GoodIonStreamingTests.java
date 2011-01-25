// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonReader;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.IonImplUtils;
import java.io.File;
import java.io.IOException;
import junit.framework.TestSuite;



public class GoodIonStreamingTests extends DirectoryTestSuite {

    public static TestSuite suite()
    {
        return new GoodIonStreamingTests();
    }

    public GoodIonStreamingTests()
    {
        super("good", "equivs");
    }

    @Override
    protected FileTestCase makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        // this test is here to get rid of the warning, and ... you never know
        if (fileName == null || fileName.length() < 1) throw new IllegalArgumentException("files should have names");
        return new GoodIonTestCase(ionFile);
    }

    @Override
    protected String[] getFilesToSkip()
    {
        return new String[]
        {
            //"equivs/stringU0001D11E.ion",
            //"equivs/symbols.ion",
        };
    }

    private static class GoodIonTestCase
        extends FileTestCase
    {

        public GoodIonTestCase(File ionFile)
        {
            super(ionFile);
        }

        @Override
        public void runTest()
            throws Exception
        {
            iterateIon(myTestFile);
        }

        void iterateIon(File myTestFile)
        throws IOException
        {
            byte[] buf = IonImplUtils.loadFileBytes(myTestFile);

            IonReader reader = system().newReader(buf);
            TestUtils.deepRead(reader);
            IonReader reader2 = system().newReader(buf);
            TestUtils.deepRead(reader2, false);
        }
    }
}
