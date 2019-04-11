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

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.GOOD_TIMESTAMP_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.testdataFiles;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.Injected.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import org.junit.Test;


public class TimestampGoodTest
    extends IonTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        testdataFiles(GLOBAL_SKIP_LIST,
                      false, /* recurse */
                      GOOD_TIMESTAMP_IONTESTS_FILES);

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }


    @Test
    public void testRoundTripFromDom()
    throws IOException
    {
        IonDatagram dg = load(myTestFile);
        Iterator<IonValue> iterator = dg.iterator();
        while (iterator.hasNext())
        {
            IonTimestamp its = (IonTimestamp) iterator.next();
            String tsText = its.toString();
            Timestamp ts = Timestamp.valueOf(tsText);
            assertEquals("timestamp", its.timestampValue(), ts);
        }
    }


    @Test
    public void testValueOf()
    throws IOException
    {
        if (! myTestFile.getName().endsWith(".ion")) return;

        String fileText = _Private_Utils.utf8FileToString(myTestFile);
        BufferedReader reader = new BufferedReader(new StringReader(fileText));

        String line = reader.readLine();
        while (line != null)
        {
            line = line.trim();
            if (line.length() != 0 && ! line.startsWith("//"))
            {
                // The line must be a valid timestamp
                Timestamp ts = Timestamp.valueOf(line);

                IonTimestamp tsDom = (IonTimestamp) system().singleValue(line);
                assertEquals("timestamp", ts, tsDom.timestampValue());
            }

            line = reader.readLine();
        }
    }
}
