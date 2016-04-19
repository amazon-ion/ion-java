/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.testdataFiles;
import static software.amazon.ion.junit.IonAssert.assertIonIteratorEquals;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonValue;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.junit.Injected.Inject;
import software.amazon.ion.streaming.ReaderCompare;

public class GoodIonTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      GOOD_IONTESTS_FILES);


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
            String ionText = PrivateUtils.utf8FileToString(myTestFile);
            loader().load(ionText);
        }
    }

    @Test
    public void testIterateByteArray()
    throws Exception
    {
        byte[] bytes = PrivateUtils.loadFileBytes(myTestFile);

        Iterator<IonValue> i = system().iterate(bytes);
        while (i.hasNext())
        {
            i.next();
        }
    }

    /**
     * Test files containing values with unknown text for symbols.
     */
    private static final String[] FILES_WITH_UNKNOWN_SYMBOL_TEXT =
                                  { "good" + File.separator + "item1.10n", "good" + File.separator + "symbols.ion" };

    /**
     * Skipping test files with unknown text for symbols.
     * This is okay because the appropriate test is covered in
     * {@link SymbolTest#testClone()}.
     */
    @Test
    public void testClone()
        throws Exception
    {
        for (String fileWithUnknownText : FILES_WITH_UNKNOWN_SYMBOL_TEXT)
        {
            if (myTestFile.getPath().endsWith(fileWithUnknownText))
            {
                return; // Skip test
            }
        }

        IonDatagram dg = loader().load(myTestFile);

        // Test on IonDatagram
        testCloneVariants(dg);

        // Test on values that are not IonDatagram
        Iterator<IonValue> iter = dg.iterator();
        while (iter.hasNext())
        {
            IonValue original = iter.next();
            testCloneVariants(original);
        }
    }

}
