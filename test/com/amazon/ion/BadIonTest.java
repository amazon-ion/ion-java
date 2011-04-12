// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Test;


public class BadIonTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES = TestUtils.testdataFiles(TestUtils.GLOBAL_SKIP_LIST, "bad");


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testLoadFile()
    throws Exception
    {
        try
        {
            load(myTestFile);
            fail("Expected IonException parsing "
                 + myTestFile.getAbsolutePath());
        }
        catch (IonException e) { /* good */ }
        catch (IOException e) { /* good?  (e.g. EOF error) */ }
    }


    @Test(expected=IonException.class)
    public void testLoadString()
    throws Exception
    {
        // attempt to load as a string
        final IonDatagram dg = loadAsJavaString(myTestFile);
        if (dg == null) {
            throw new IonException("Not valid UTF-8");
        }
    }
}
