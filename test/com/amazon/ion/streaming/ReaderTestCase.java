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

package com.amazon.ion.streaming;

import com.amazon.ion.InputStreamWrapper;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonType;
import com.amazon.ion.ReaderChecker;
import com.amazon.ion.ReaderMaker;
import com.amazon.ion.junit.IonAssert;
import java.io.IOException;
import org.junit.After;



public abstract class ReaderTestCase
    extends IonTestCase
{
    protected ReaderMaker myReaderMaker;
    protected IonReader in;

    public void setReaderMaker(ReaderMaker maker)
    {
        myReaderMaker = maker;
    }

    @After @Override
    public void tearDown() throws Exception
    {
        in = null;
        super.tearDown();
    }

    //========================================================================

    void read(byte[] ionData)
    {
        in = myReaderMaker.newReader(system(), ionData);
    }

    void read(byte[] ionData, InputStreamWrapper wrapper)
        throws IOException
    {
        in = myReaderMaker.newReader(system(), ionData, wrapper);
    }

    void read(String ionText)
    {
        in = myReaderMaker.newReader(system(), ionText);
    }

    //========================================================================

    protected ReaderChecker check()
    {
        return new ReaderChecker(in);
    }


    protected void expectNoCurrentValue()
    {
        IonAssert.assertNoCurrentValue(in);
    }

    protected void expectTopLevel()
    {
        IonAssert.assertTopLevel(in);
    }

    protected void expectEof()
    {
        IonAssert.assertEof(in);
    }

    protected void expectTopEof()
    {
        IonAssert.assertTopEof(in);
    }


    protected void expectNextField(String name)
    {
        check().next().fieldName(name);
    }

    protected void expectString(String text)
    {
        assertEquals("getType", IonType.STRING, in.getType());
        assertEquals("isNullValue", text == null, in.isNullValue());
        assertEquals("stringValue", text, in.stringValue());
    }
}
