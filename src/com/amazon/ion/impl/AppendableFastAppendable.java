// Copyright (c) 2013-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.util.PrivateFastAppendable;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * Adapts an {@link Appendable} to implement {@link PrivateFastAppendable}.
 */
final class AppendableFastAppendable
    implements PrivateFastAppendable, Closeable, Flushable
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
