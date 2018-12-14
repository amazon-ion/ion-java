/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package software.amazon.ion;

import static software.amazon.ion.TestUtils.BAD_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.testdataFiles;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import org.junit.Test;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.junit.Injected.Inject;


public class BadIonTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST, BAD_IONTESTS_FILES);


    private File myTestFile;
    private boolean myFileIsBinary;

    public void setTestFile(File file)
    {
        myTestFile = file;

        String fileName = file.getName();
        myFileIsBinary = fileName.endsWith(".10n");
    }


    @Test
    public void testLoadFile()
    throws Exception
    {
        try
        {
            load(myTestFile);
            fail("Expected exception");
        }
        catch (IonException e) { /* good */ }
        catch (IOException e) { /* good?  (e.g. EOF error) */ }
    }


    @Test
    public void testLoadString()
    throws Exception
    {
        if (! myFileIsBinary)
        {
            String ionText;
            try
            {
                // This will fail if the file has bad UTF-8 data.
                // That's OK, the other test will still do the right thing.
                ionText = PrivateUtils.utf8FileToString(myTestFile);
            }
            catch (IonException e)
            {
                // checks that test failed because of bad UTF-8 data
                final CharBuffer buffer = CharBuffer.allocate(1204);
                final CharsetEncoder utf8Encoder = Charset.forName("UTF-8").newEncoder();

                final FileReader fileReader = new FileReader(myTestFile);
                while(fileReader.read(buffer) != -1){
                    assert utf8Encoder.canEncode(buffer);
                }

                return;
            }

            try
            {
                @SuppressWarnings("unused")
                IonDatagram dg = loader().load(ionText);

                fail("Expected IonException");
            }
            catch (IonException e) { /* good */ }
        }
    }
}
