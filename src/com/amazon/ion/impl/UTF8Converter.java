// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class UTF8Converter
    implements Flushable, Closeable, Appendable
{
    private static final int MAX_BYTES_LEN = 4096;
    // this is a temporary byte buffer where the generated data is written
    // before it gets to OutputStream
    private int bytePos_;
    private byte[] bytes_;
    // OutputStream
    private OutputStream out_;

    public UTF8Converter(OutputStream out) {
        out.getClass();
        this.bytePos_ = 0;
        this.bytes_ = new byte[MAX_BYTES_LEN];
        this.out_ = out;
    }

    public int writeBytes(byte[] value, int start, int end) throws IOException {
        if (bytePos_ > 0) {
            out_.write(bytes_, 0, bytePos_);
            bytePos_ = 0;
        }
        out_.write(value, start, end - start);
        return end - start;
    }

    /**
     * Write single char
     */
    public Appendable append(char c) throws IOException {
        if (bytePos_ + 3 > bytes_.length) {
            out_.write(bytes_, 0, bytePos_);
            bytePos_ = 0;
        }
        if (c < 0x80) {
            bytes_[bytePos_++] = (byte)c;
        } else if (c < 0x800) {
            // 2 byte UTF8
            bytes_[bytePos_++] = (byte)( 0xFF & (0xC0 | ((c >> 6)        )) );
            bytes_[bytePos_++] = (byte)( 0xFF & (0x80 | ((c     )  & 0x3F)) );
        } else if (c < 0xD800 || c > 0xDFFF) {
            // 3 byte UTF8
            bytes_[bytePos_++] = (byte)( 0xff & (0xE0 | ((c >> 12)       )) );
            bytes_[bytePos_++] = (byte)( 0xff & (0x80 | ((c >>  6) & 0x3F)) );
            bytes_[bytePos_++] = (byte)( 0xff & (0x80 | ((c      ) & 0x3F)) );
        } else {
            throw new IonException("invalid string, unpaired high surrogate single character " + (int)c);
        }
        return this;
    }

    public Appendable append(CharSequence seq) throws IOException
    {
        return append(seq, 0, seq.length());
    }

    public Appendable append(CharSequence seq, int start, int end) throws IOException
    {
        while (start < end) {
            int c = seq.charAt(start++);
            if (bytePos_ + 4 > bytes_.length) {
                out_.write(bytes_, 0, bytePos_);
                bytePos_ = 0;
            }
            if (c < 0x80) {
                // 1 byte UTF8
                bytes_[bytePos_++] = (byte)c;
            } else if (c < 0x800) {
                // 2 byte UTF8
                bytes_[bytePos_++] = (byte)( 0xFF & (0xC0 | ((c >> 6)        )) );
                bytes_[bytePos_++] = (byte)( 0xFF & (0x80 | ((c     )  & 0x3F)) );
            } else if (c < 0xD800 || c >= 0xE000) {
                // 3 byte UTF8
                bytes_[bytePos_++] = (byte)( 0xff & (0xE0 | ((c >> 12)       )) );
                bytes_[bytePos_++] = (byte)( 0xff & (0x80 | ((c >>  6) & 0x3F)) );
                bytes_[bytePos_++] = (byte)( 0xff & (0x80 | ((c      ) & 0x3F)) );
            } else if (c >= 0xD800 && c < 0xDC00) {
                // high surrogate, must be followed by low
                if (start == end) {
                    // there are no more bytes left in the string. it is malformed!
                    throw new IonException("invalid string, unpaired high surrogate character");
                }
                int c2 = seq.charAt(start++);
                if (c2 < 0xDC00 || c2 > 0xDFFF) {
                    throw new IonException("invalid string, unpaired high surrogate character");
                }
                c = ((c - 0xD800) << 10) + (c2 - 0xDC00) + 0x10000;
                if (c > 0x10FFFF) {
                    throw new IonException("Invalid Unicode scalar " + c);
                }
                bytes_[bytePos_++] = (byte)( 0xFF & (0xF0 | ((c >> 18)       )) );
                bytes_[bytePos_++] = (byte)( 0xFF & (0x80 | ((c >> 12) & 0x3F)) );
                bytes_[bytePos_++] = (byte)( 0xFF & (0x80 | ((c >>  6) & 0x3F)) );
                bytes_[bytePos_++] = (byte)( 0xFF & (0x80 | ((c      ) & 0x3F)) );
            } else {
                // there are no more bytes left in the string. it is malformed!
                throw new IonException("invalid string, unpaired low surrogate character");
            }
        }
        return this;
    }

    public void flush() throws IOException {
        if (bytePos_ > 0) {
            out_.write(bytes_, 0, bytePos_);
            bytePos_ = 0;
        }
        out_.flush();
    }

    public void close() throws IOException  {
        try {
            flush();
        } finally {
            out_.close();
        }
    }


}
