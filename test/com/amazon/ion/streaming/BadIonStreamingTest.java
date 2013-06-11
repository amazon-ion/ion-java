// Copyright (c) 2008-2013 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.streaming;

import static com.amazon.ion.TestUtils.BAD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;

public class BadIonStreamingTest
extends IonTestCase
{
    private static final boolean _debug_output_errors = false;


    @Inject("testFile")
    public static final File[] FILES = testdataFiles(GLOBAL_SKIP_LIST,
                                                     BAD_IONTESTS_FILES);


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test(expected = IonException.class)
    public void testReadingScalars()
    throws Exception
    {
        readFile( true );
    }


    @Ignore // TODO ION-161
    @Test(expected = IonException.class)
    public void testSkippingScalars()
    throws Exception
    {
        if (getDomType() != DomType.LITE)
        {
            // Newer readers don't validate while skipping scalars
            // so we won't throw exceptions for all bad files.
            readFile( false );
        }
    }

    private void readFile(boolean materializeScalars)
    throws IOException
    {
        try
        {
            byte[] buf = _Private_Utils.loadFileBytes(myTestFile);
            IonReader it = system().newReader(buf);
            TestUtils.deepRead(it, materializeScalars);
        }
        catch (IonException e)
        {
            /* good - we're expecting an error, there are testing bad input */
            if (_debug_output_errors) {
                System.out.print(this.myTestFile.getName());
                System.out.print(": ");
                System.out.println(e.getMessage());
            }
            throw e;
        }
    }
}
