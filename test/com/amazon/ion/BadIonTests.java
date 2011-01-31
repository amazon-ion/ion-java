// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import org.junit.Test;


public class BadIonTests
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES = TestUtils.testdataFiles("bad");


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void test()
    throws Exception
    {
        try
        {
            load(myTestFile);
            fail("Expected IonException parsing "
                 + myTestFile.getAbsolutePath());
        }
        catch (IonException e) { /* good */ }

        try
        {
            // attempt to load as a string
            final IonDatagram dg = loadAsJavaString(myTestFile);
            if (dg != null) {
                fail("Expected IonException parsing as a Java String "
                     + myTestFile.getAbsolutePath());
            }
        }
        catch (IonException e) { /* good */ }
    }
}
