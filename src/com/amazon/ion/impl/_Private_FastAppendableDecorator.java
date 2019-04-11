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

package com.amazon.ion.impl;

import com.amazon.ion.util._Private_FastAppendable;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 */
public abstract class _Private_FastAppendableDecorator
    implements _Private_FastAppendable, Closeable, Flushable
{
    private final _Private_FastAppendable myOutput;

    public _Private_FastAppendableDecorator(_Private_FastAppendable output) {
        myOutput = output;
    }

    public void flush()
        throws IOException
    {
        if (myOutput instanceof Flushable) {
            ((Flushable) myOutput).flush();
        }
    }

    public void close()
        throws IOException
    {
        if (myOutput instanceof Closeable) {
            ((Closeable) myOutput).close();
        }
    }

    public Appendable append(char c)
        throws IOException
    {
        myOutput.append(c);
        return this;
    }

    public Appendable append(CharSequence csq)
        throws IOException
    {
        myOutput.append(csq);
        return this;
    }

    public Appendable append(CharSequence csq, int start, int end)
        throws IOException
    {
        myOutput.append(csq, start, end);
        return this;
    }

    public void appendAscii(char c)
        throws IOException
    {
        myOutput.appendAscii(c);
    }

    public void appendAscii(CharSequence csq)
        throws IOException
    {
        myOutput.appendAscii(csq);
    }

    public void appendAscii(CharSequence csq, int start, int end)
        throws IOException
    {
        myOutput.appendAscii(csq, start, end);
    }

    public void appendUtf16(char c)
        throws IOException
    {
        myOutput.appendUtf16(c);
    }

    public void appendUtf16Surrogate(char leadSurrogate, char trailSurrogate)
        throws IOException
    {
        myOutput.appendUtf16Surrogate(leadSurrogate, trailSurrogate);
    }
}
