// Copyright (c) 2013-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Implementation of {@link _Private_IonTextAppender} that writes to an
 * {@link Appendable}.
 */
final class AppendableIonTextAppender
    extends _Private_IonTextAppender
{
    private final Appendable _out;

    AppendableIonTextAppender(Appendable out, Charset charset)
    {
        super(charset.equals(_Private_Utils.ASCII_CHARSET));
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
