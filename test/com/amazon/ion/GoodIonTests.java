// Copyright (c) 2007-2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.streaming.ReaderCompare;
import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
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

        @Override
        public void runTest()
            throws Exception
        {
            // Pass 1: Use Loader to read the data
            IonDatagram datagram = load(myTestFile);


            // Pass 2: Use IonReader
            IonReader treeReader = system().newReader(datagram);

            FileInputStream in = new FileInputStream(myTestFile);
            try {
                IonReader fileReader = system().newReader(in);

                ReaderCompare.compare(treeReader, fileReader);
            }
            finally {
                in.close();
            }


            // Pass 3: Use Iterator
            in = new FileInputStream(myTestFile);
            try {
                Iterator<IonValue> i = system().iterate(in);

                Iterator<IonValue> expected = datagram.iterator();

                TestUtils.assertEqualValues(expected, i);
            }
            finally {
                in.close();
            }


            // Pass 4: Encode to binary, and use Reader
            if (! myFileIsBinary) {
                // Check the encoding of text to binary.
                treeReader = system().newReader(datagram);

                byte[] encoded = datagram.getBytes();
                IonReader binaryReader = system().newReader(encoded);

                ReaderCompare.compare(treeReader, binaryReader);
            }
        }
    }


    public static TestSuite suite()
    {
        return new GoodIonTests();
    }


    public GoodIonTests()
    {
        super("good", "equivs");
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

    @Override
    protected String[] getFilesToSkip()
    {
        return new String[]
        {
//            "equivs/stringU0001D11E.ion",
//            "equivs/stringU0120.ion",
//            "equivs/stringU2021.ion",
//            "equivs/symbols.ion",
//            "equivs/textNewlines.ion",
        };
    }
}
