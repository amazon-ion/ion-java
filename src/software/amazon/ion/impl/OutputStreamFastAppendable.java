/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static software.amazon.ion.impl.PrivateIonConstants.makeUnicodeScalar;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import software.amazon.ion.util.PrivateFastAppendable;

/**
 * Adapts an {@link OutputStream} to implement {@link PrivateFastAppendable}.
 * <b>This always outputs UTF-8!</b>
 */
final class OutputStreamFastAppendable
    implements PrivateFastAppendable, Closeable, Flushable
{
    private static final int MAX_BYTES_LEN = 4096;

    private final OutputStream _out;

    /** Aggregates bytes so we can write to {@link #_out} in large batches. */
    private final byte[] _byteBuffer;

    /** Position in {@link #_byteBuffer} where we'll write the next byte. */
    private int _pos;

    OutputStreamFastAppendable(OutputStream out)
    {
        out.getClass(); // Efficient null check

        _out = out;
        _pos = 0;
        _byteBuffer = new byte[MAX_BYTES_LEN];
    }

    // ------------------- FastAppendable Appendable Methods -------------------
    public Appendable append(char c)
        throws IOException
    {
        // Choose what method to use depending on type of character.
        if (c < 0x80) {
            appendAscii(c);
        } else {
            appendUtf16(c);
        }
        return this;
    }

    public Appendable append(CharSequence csq)
        throws IOException
    {
        append(csq, 0, csq.length());
        return this;
    }

    public Appendable append(CharSequence csq, int start, int end)
        throws IOException
    {
        for (int ii = start; ii < end; ++ii) {
            append(csq.charAt(ii));
        }
        return this;
    }

    public final void appendAscii(char c)
        throws IOException
    {
        if (_pos == _byteBuffer.length) {
            _out.write(_byteBuffer, 0, _pos);
            _pos = 0;
        }
        assert c < 0x80;
        _byteBuffer[_pos++] = (byte)c;
    }

    public final void appendAscii(CharSequence csq)
        throws IOException
    {
        appendAscii(csq, 0, csq.length());
    }

    @SuppressWarnings("deprecation")
    public final void appendAscii(CharSequence csq, int start, int end)
        throws IOException
    {
        if (csq instanceof String) {
            // Using deprecated String.getBytes intentionally, since it is
            // correct behavior in this case, and much faster.
            String str = (String)csq;
            int len = end - start;
            if (_pos + len < _byteBuffer.length) {
                // put String bytes directly into buffer
                str.getBytes(start, end, _byteBuffer, _pos);
                _pos += len;
            } else {
                do {
                    // flush the buffer on every loop
                    _out.write(_byteBuffer, 0, _pos);
                    // check if we still need to split into chunks
                    _pos = (end - start > _byteBuffer.length
                                ? _byteBuffer.length
                                : end - start);
                    str.getBytes(start, start + _pos, _byteBuffer, 0);
                    start += _pos;
                } while (start < end);
            }
        } else {
            for (int ii=start; ii < end; ii++) {
                if (_pos == _byteBuffer.length) {
                    _out.write(_byteBuffer, 0, _pos);
                    _pos = 0;
                }
                char c = csq.charAt(ii);
                assert c < 0x80;
                _byteBuffer[_pos++] = (byte)c;
            }
        }
    }

    public final void appendUtf16(char c)
        throws IOException
    {
        assert c >= 0x80;

        if (_pos > _byteBuffer.length - 3) {
            _out.write(_byteBuffer, 0, _pos);
            _pos = 0;
        }

        if (c < 0x800) {
            _byteBuffer[_pos++] = (byte)( 0xff & (0xC0 | ( c >> 6        )) );
            _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ( c       & 0x3F)) );
        } else if (c < 0x10000) {
            _byteBuffer[_pos++] = (byte)( 0xff & (0xE0 | ( c >> 12       )) );
            _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) );
            _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ( c       & 0x3F)) );
        }
    }

    public final void appendUtf16Surrogate(char leadSurrogate,
                                           char trailSurrogate)
        throws IOException
    {
        // Here we must convert a UTF-16 surrogate pair to UTF-8 bytes.

        int c = makeUnicodeScalar(leadSurrogate, trailSurrogate);
        assert c >= 0x10000;

        if (_pos > _byteBuffer.length - 4) {
            _out.write(_byteBuffer, 0, _pos);
            _pos = 0;
        }

        _byteBuffer[_pos++] = (byte)( 0xff & (0xF0 | ( c >> 18        )) );
        _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ((c >> 12) & 0x3F)) );
        _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ((c >> 6)  & 0x3F)) );
        _byteBuffer[_pos++] = (byte)( 0xff & (0x80 | ( c        & 0x3F)) );
    }


    public final void flush()
        throws IOException
    {
        if (_pos > 0) {
            _out.write(_byteBuffer, 0, _pos);
            _pos = 0;
        }
        _out.flush();
    }

    public final void close()
        throws IOException
    {
        try
        {
            flush();
        }
        finally
        {
            _out.close();
        }
    }
}
