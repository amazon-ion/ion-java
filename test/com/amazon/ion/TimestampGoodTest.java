// Copyright (c) 2011-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_TIMESTAMP_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import org.junit.Test;

/**
 *
 */
public class TimestampGoodTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      false, /* recurse */
                      GOOD_TIMESTAMP_IONTESTS_FILES);

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testRoundTripFromDom()
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


    @Test
    public void testValueOf()
    throws IOException
    {
        if (! myTestFile.getName().endsWith(".ion")) return;

        String fileText = _Private_Utils.utf8FileToString(myTestFile);
        BufferedReader reader = new BufferedReader(new StringReader(fileText));

        String line = reader.readLine();
        while (line != null)
        {
            line = line.trim();
            if (line.length() != 0 && ! line.startsWith("//"))
            {
                // The line must be a valid timestamp
                Timestamp ts = Timestamp.valueOf(line);

                IonTimestamp tsDom = (IonTimestamp) system().singleValue(line);
                assertEquals("timestamp", ts, tsDom.timestampValue());
            }

            line = reader.readLine();
        }
    }
}
