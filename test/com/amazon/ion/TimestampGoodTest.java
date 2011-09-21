// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.junit.Test;

/**
 *
 */
public class TimestampGoodTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES = testdataFiles(GLOBAL_SKIP_LIST,
                                                     "good/timestamp");

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testValueOf()
    throws IOException
    {
        IonDatagram dg = load(myTestFile);
        Iterator<IonValue> iterator = dg.iterator();
        while (iterator.hasNext())
        {
            IonTimestamp its = (IonTimestamp) iterator.next();
            String tsText = its.toString();
            Timestamp ts = Timestamp.valueOf(tsText);
            assertEquals("timestamp", its.timestampValue(), ts);
        }
    }
}
