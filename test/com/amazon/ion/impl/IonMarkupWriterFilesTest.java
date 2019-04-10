
/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.ion.impl;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonWriter;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util.NullOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.junit.Test;

public class IonMarkupWriterFilesTest
        extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES = testdataFiles(GLOBAL_SKIP_LIST,
                                             GOOD_IONTESTS_FILES);
    private File               myTestFile;

    public void setTestFile(File file) {
        myTestFile = file;
    }

    /**
     * Tests to make sure that the type, the output stream, and the
     * annotation/field name aren't null for all the files. This will fail if
     * the callback type isn't being set.
     * {@link IonMarkupWriterTest#testStandardCallback()} should verify they're
     * set correctly.
     */
    @Test
    public void testPrintingStandard()
        throws Exception
    {
        // Just ignore the output, since we're not checking it
        OutputStream out = new NullOutputStream();
        InputStream in = new FileInputStream(myTestFile);

        // Get a Test markup writer
        _Private_IonTextWriterBuilder builder = (_Private_IonTextWriterBuilder)
            IonTextWriterBuilder.standard();
        IonWriter ionWriter = builder
            .withCallbackBuilder(new TestMarkupCallback.Builder())
            .build(out);

        // Load file into a reader
        IonReader ionReader = system().newReader(in);

        assertNotNull("ionReader is null", ionReader);
        assertNotNull("ionWriter is null", ionWriter);

        // Write data to the writer
        ionWriter.writeValues(ionReader);
        // Close the writer and reader
        ionWriter.close();
        ionReader.close();
    }
}
