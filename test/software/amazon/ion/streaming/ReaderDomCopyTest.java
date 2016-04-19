/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static software.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static software.amazon.ion.TestUtils.testdataFiles;
import static software.amazon.ion.junit.IonAssert.assertIonEquals;

import java.io.File;
import java.io.IOException;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.IonValue;
import software.amazon.ion.junit.Injected.Inject;

public class ReaderDomCopyTest
extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST, GOOD_IONTESTS_FILES);


    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }



    @Test
    public void test()
    throws Exception
    {
        iterateIon(myTestFile);
    }

    void iterateIon(File myTestFile)
    throws IOException
    {
        IonDatagram dg = system().getLoader().load(myTestFile);

        IonReader reader = system().newReader(dg);
        for (int i = 0; reader.next() != null; i++)
        {
            IonValue actual = system().newValue(reader);
            assertIonEquals(dg.get(i), actual);
        }
    }
}
