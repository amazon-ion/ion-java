// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class GoodIonStreamingTest
extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      GOOD_IONTESTS_FILES);


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
        byte[] buf = _Private_Utils.loadFileBytes(myTestFile);

        IonReader reader = system().newReader(buf);
        TestUtils.deepRead(reader);
        IonReader reader2 = system().newReader(buf);
        TestUtils.deepRead(reader2, false);
    }
}
