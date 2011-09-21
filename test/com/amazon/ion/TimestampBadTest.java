// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import org.junit.Test;

/**
 *
 */
public class TimestampBadTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES = testdataFiles(GLOBAL_SKIP_LIST,
                                                     "bad/timestamp");

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testValueOf()
    throws IOException
    {
        String tsText;
        try
        {
            tsText = IonImplUtils.utf8FileToString(myTestFile);
        }
        catch (IonException e)
        {
            // Bad UTF-8 data, just ignore the file
            return;
        }

        tsText = new BufferedReader(new StringReader(tsText)).readLine();
        tsText = tsText.trim();  // Trim newlines and whitespace

        // TODO some bad files have comments in them

        try
        {
            Timestamp.valueOf(tsText);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
    }
}
