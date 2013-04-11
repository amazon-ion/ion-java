// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 */
public class UTF8Converter
{
    private static final int MAX_CHARS_LEN = 1024;
    // char buffer length
    private int charsLen;
    // one char can be represented as 4 bytes, eg 4x the chars
    private int bytesLen;
    // this is a temporary char buffer that is filled from CharSequence
    // to leverage the array iteration rather than using charAt()
    private char[] chars_;
    // this is a temporary byte buffer where the generated data is written
    // before it gets to OutputStream
    private byte[] bytes_;

    public UTF8Converter() {
        this(MAX_CHARS_LEN);
    }

    public UTF8Converter(int charsLen) {
        if (charsLen > MAX_CHARS_LEN) {
            charsLen = MAX_CHARS_LEN;
        }
        this.charsLen = charsLen;
        this.bytesLen = 4 * charsLen;
        this.chars_ = new char[this.charsLen];
        this.bytes_ = new byte[this.bytesLen];
    }

    /**
     * Write single char
     */
    public int write(OutputStream out, char c) throws IOException {
        if (c < 0x80) {
            out.write((byte)c);
            return 1;
        }
        if (c >= 0xD800 && c <= 0xDFFF) {
            throw new IonException("invalid string, unpaired high surrogate single character");
        }
        int len = writeUnicodeScalarToByteBuffer(c, bytes_, 0);
        out.write(bytes_, 0, len);
        return len;
    }
    /**
     * Write @str string into @out stream and return number of bytes written
     * @param out Target OutputStream
     * @param str String to write
     * @return Number of bytes written
     */
    public int write(OutputStream out, String str) throws IOException {
        return write(out, str, 0, str.length());
    }

    /**
     * Write @str string into @out stream and return number of bytes written
     * @param out Target OutputStream
     * @param str String to write
     * @param off Start offset in the sequence
     * @param len Length of the sequence
     * @return Number of bytes written
     */
    public int write(OutputStream out, String str, int off, int len) throws IOException {
        int lengthWritten = 0;

        len += off;             // last byte to read
        int start = off;        // start byte
        while (start < len) {
            // calculate the last char to read from @seq
            int end = start + charsLen;
            if (end > len) {
                end = len;
            }
            // buffer the sequence
            str.getChars(start, end, chars_, 0);
            // write the chunk
            int charPos = 0;
            int bytePos = 0;
            int charLen = end - start;
            while (charPos < charLen) {
                int c = chars_[charPos++];
                if (c < 0x80) {
                    bytes_[bytePos++] = (byte)c;
                    continue;
                }
                if (c >= 0xD800 && c <= 0xDFFF) {
                    if (_Private_IonConstants.isHighSurrogate(c)) {
                        if (charPos == charLen) {
                            // the high surrogate happen to fall as the last byte of the sequence
                            if (end == len) {
                                // there are no more bytes left in the string. it is malformed!
                                throw new IonException("invalid string, unpaired high surrogate character");
                            }
                            // revert the byte we've read and read the next chunk in outer loop
                            --charPos;
                            break;
                        }
                        int c2 = chars_[charPos++];
                        if (!_Private_IonConstants.isLowSurrogate(c2)) {
                            throw new IonException("invalid string, unpaired high surrogate character");
                        }
                        c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                    }
                    else if (_Private_IonConstants.isLowSurrogate(c)) {
                        // it's a loner low surrogate - that's an error
                        throw new IonException("invalid string, unpaired low surrogate character");
                    }
                    // from 0xE000 up the _writeUnicodeScalar will check for us
                }
                bytePos += writeUnicodeScalarToByteBuffer(c, bytes_, bytePos);
            }
            // flush the written bytes to output stream
            out.write(bytes_, 0, bytePos);
            // increment @start position
            start += charPos;
            // increment total length writter
            lengthWritten += bytePos;
        }
        return lengthWritten;
    }

    /**
     * Write a Unicode scalar @c (c >= 0x80!) into the given byte @buffer at @offset and return number of bytes
     * written
     */
    private final int writeUnicodeScalarToByteBuffer(int c, byte[] buffer, int offset) throws IOException {
        assert c >= 0x80;
        assert offset + 4 <= buffer.length;

        if (c < 0x800) {
            // 2 bytes characters from 0x000080 to 0x0007FF
            buffer[offset] = (byte)( 0xff & (0xC0 | (c >> 6)) );
            buffer[++offset] = (byte)( 0xff & (0x80 | (c & 0x3F)) );
            return 2;
        }
        else if (c < 0x10000) {
            // 3 byte characters from 0x800 to 0xFFFF
            // but only 0x800...0xD7FF and 0xE000...0xFFFF are valid
            buffer[offset] = (byte)( 0xff & (0xE0 |  (c >> 12)) );
            buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) );
            buffer[++offset] = (byte)( 0xff & (0x80 |  (c & 0x3F)) );
            return 3;
        }
        else if (c <= 0x10FFFF) {
            // 4 byte characters 0x010000 to 0x10FFFF
            // these are are valid
            buffer[offset] = (byte)( 0xff & (0xF0 |  (c >> 18)) );
            buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 12) & 0x3F)) );
            buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) );
            buffer[++offset] = (byte)( 0xff & (0x80 | (c & 0x3F)) );
            return 4;
        }
        throw new IonException("Invalid Unicode scalar " + c);
    }

}
