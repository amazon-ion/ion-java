// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.testdataFiles;
import static com.amazon.ion.junit.IonAssert.assertIonIteratorEquals;

import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.streaming.ReaderCompare;
import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import org.junit.Test;


public class GoodIonTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(TestUtils.GLOBAL_SKIP_LIST,
                      "good", "equivs");


    private File myTestFile;
    private boolean myFileIsBinary;

    public void setTestFile(File file)
    {
        myTestFile = file;

        String fileName = file.getName();
        myFileIsBinary = fileName.endsWith(".10n");
    }


    @Test
    public void test()
    throws Exception
    {
        if (myTestFile.getName().startsWith("__")) {
            System.out.println("debug file encountered: "+myTestFile.getName());
        }

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
            Iterator<IonValue> expected = datagram.iterator();
            Iterator<IonValue> actual = system().iterate(in);

            assertIonIteratorEquals(expected, actual);
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


    @Test
    public void testLoadString()
    throws Exception
    {
        if (! myFileIsBinary)
        {
            // Pull the file into a String, then load it.
            loadAsJavaString(myTestFile);
        }
    }

    @Test
    public void testIterateByteArray()
    throws Exception
    {
        byte[] bytes = IonImplUtils.loadFileBytes(myTestFile);

        Iterator<IonValue> i = system().iterate(bytes);
        while (i.hasNext())
        {
            i.next();
        }
    }
}
