// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion;

import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Test;


public class BadIonTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(TestUtils.GLOBAL_SKIP_LIST, "bad");


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
            String ionText = IonImplUtils.utf8FileToString(myTestFile);

            try
            {
                IonDatagram dg = loader().load(ionText);

                // Flush out any encoding problems in the data.
                //forceMaterialization(dg);

                fail("Expected IonException");
            }
            catch (IonException e) { /* good */ }
        }
    }
}
