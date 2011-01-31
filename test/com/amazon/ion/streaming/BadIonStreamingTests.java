// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.streaming;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.system.SystemFactory.SystemCapabilities;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class BadIonStreamingTests
extends IonTestCase
{
    private static final boolean _debug_output_errors = false;


    @Inject("testFile")
    public static final File[] FILES = TestUtils.testdataFiles("bad");


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testReading()
    throws Exception
    {
        iterateIon( true );

        if (getSystemCapabilities() != SystemCapabilities.LITE)
        {
            // "Lite" system doesn't validate while skipping scalars
            // so we won't throw exceptions for all bad files.
            iterateIon( false );
        }
    }

    void iterateIon(boolean materializeScalars)
    throws IOException
    {
        try {
            byte[] buf = IonImplUtils.loadFileBytes(myTestFile);

            // Do we want to check the type of "it" here
            // to make sure the iterator made the right
            // choice of binary or text?  Or should be test
            // that somewhere else?
            IonReader it = system().newReader(buf);
            TestUtils.deepRead(it, materializeScalars);
            fail("Expected IonException parsing "
                 + myTestFile.getAbsolutePath() + " (" + ( materializeScalars ? "" : "not " ) + "materializing scalars)");
        } catch (IonException e) {
            /* good - we're expecting an error, there are testing bad input */
            if (_debug_output_errors) {
                System.out.print(this.myTestFile.getName());
                System.out.print(": ");
                System.out.println(e.getMessage());
            }
        }
    }
}
