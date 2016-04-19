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

package software.amazon.ion.impl;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import software.amazon.ion.util.PrivateFastAppendable;

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
