/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import static com.amazon.ion.TestUtils.BAD_TIMESTAMP_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import org.junit.Test;


public class TimestampBadTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST, BAD_TIMESTAMP_IONTESTS_FILES);

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testValueOf()
    throws IOException
    {
        String tsText;
        try
        {
            tsText = _Private_Utils.utf8FileToString(myTestFile);
        }
        catch (IonException e)
        {
            // Bad UTF-8 data, just ignore the file
            return;
        }

        tsText = new BufferedReader(new StringReader(tsText)).readLine();
        tsText = tsText.trim();  // Trim newlines and whitespace

        // TODO some bad files have comments in them

        try
        {
            Timestamp.valueOf(tsText);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
    }
}
