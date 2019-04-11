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
 * Adapts an {@link Appendable} to implement {@link _Private_FastAppendable}.
 */
final class AppendableFastAppendable
    implements _Private_FastAppendable, Closeable, Flushable
{
    private final Appendable _out;

    AppendableFastAppendable(Appendable out)
    {
        out.getClass(); // Efficient null check

        _out = out;
    }

    public Appendable append(CharSequence csq)
        throws IOException
    {
        _out.append(csq);
        return this;
    }

    public Appendable append(char c)
        throws IOException
    {
        _out.append(c);
        return this;
    }

    public Appendable append(CharSequence csq, int start, int end)
        throws IOException
    {
        _out.append(csq, start, end);
        return this;
    }

    public final void appendAscii(char c)
        throws IOException
    {
        _out.append(c);
    }

    public final void appendAscii(CharSequence csq)
        throws IOException
    {
        _out.append(csq);
    }

    public final void appendAscii(CharSequence csq, int start, int end)
        throws IOException
    {
        _out.append(csq, start, end);
    }

    public final void appendUtf16(char c)
        throws IOException
    {
        _out.append(c);
    }

    public final void appendUtf16Surrogate(char leadSurrogate,
                                           char trailSurrogate)
        throws IOException
    {
        _out.append(leadSurrogate);
        _out.append(trailSurrogate);
    }

    public void flush()
        throws IOException
    {
        if (_out instanceof Flushable)
        {
            ((Flushable)_out).flush();
        }
    }

    public void close() throws IOException
    {
        if (_out instanceof Closeable)
        {
            ((Closeable)_out).close();
        }
    }
}
