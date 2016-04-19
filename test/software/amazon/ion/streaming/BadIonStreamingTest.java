/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.ion.streaming;

import static software.amazon.ion.TestUtils.BAD_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.testdataFiles;

import java.io.File;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.TestUtils;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.junit.Injected.Inject;

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


    @Ignore // TODO amznlabs/ion-java#7
    @Test(expected = IonException.class)
    public void testSkippingScalars()
    throws Exception
    {
        // Readers don't validate while skipping scalars
        // so we won't throw exceptions for all bad files.
        readFile( false );
    }

    private void readFile(boolean materializeScalars)
    throws IOException
    {
        try
        {
            byte[] buf = PrivateUtils.loadFileBytes(myTestFile);
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
