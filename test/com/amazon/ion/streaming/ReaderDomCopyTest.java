// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;
import static com.amazon.ion.junit.IonAssert.assertIonEquals;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonValue;
import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

/** Based on ION-297 */
public class ReaderDomCopyTest
extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GOOD_IONTESTS_FILES);


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }



    @Test
    public void test()
    throws Exception
    {
        iterateIon(myTestFile);
    }

    void iterateIon(File myTestFile)
    throws IOException
    {
        IonDatagram dg = system().getLoader().load(myTestFile);

        IonReader reader = system().newReader(dg);
        for (int i = 0; reader.next() != null; i++)
        {
            IonValue actual = system().newValue(reader);
            assertIonEquals(dg.get(i), actual);
        }
    }
}
