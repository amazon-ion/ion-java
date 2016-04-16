// Copyright (c) 2013-2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.util.PrivateFastAppendable;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public abstract class PrivateFastAppendableDecorator
    implements PrivateFastAppendable, Closeable, Flushable
{
    private final PrivateFastAppendable myOutput;

    public PrivateFastAppendableDecorator(PrivateFastAppendable output) {
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
