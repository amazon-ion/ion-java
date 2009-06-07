// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
        // TODO JIRA ION-8 fix Unicode bugs and enable test cases
        return new String[]
        {
            "equivs/stringU0001D11E.ion",
            "equivs/symbols.ion",
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

        void iterateIon(File myTestFile) {
            int len = (int)myTestFile.length();
            byte[] buf = new byte[len];

            FileInputStream in;
            BufferedInputStream bin;
            try {
                in = new FileInputStream(myTestFile);
                bin = new BufferedInputStream(in);
                bin.read(buf);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

            IonReader reader = system().newReader(buf);
            readEverything(reader);
        }

        void readEverything(IonReader it) {
            while (it.hasNext()) {
                IonType t = it.next();
                switch (t) {
                    case NULL:
                    case BOOL:
                    case INT:
                    case FLOAT:
                    case DECIMAL:
                    case TIMESTAMP:
                    case STRING:
                    case SYMBOL:
                    case BLOB:
                    case CLOB:
                        break;
                    case STRUCT:
                    case LIST:
                    case SEXP:
                        it.stepIn();
                        readEverything(it);
                        it.stepOut();
                        break;
                    case DATAGRAM:
                        fail("datagram not expected");
                }
            }
            return;
        }
    }
}
