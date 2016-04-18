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

package com.amazon.ion.streaming;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.PrivateUtils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class GoodIonStreamingTest
extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      GOOD_IONTESTS_FILES);


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
        byte[] buf = PrivateUtils.loadFileBytes(myTestFile);

        IonReader reader = system().newReader(buf);
        TestUtils.deepRead(reader);
        IonReader reader2 = system().newReader(buf);
        TestUtils.deepRead(reader2, false);
    }
}
