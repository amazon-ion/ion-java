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

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.Timestamp;
import com.amazon.ion.Timestamp.Precision;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * Manages a very simple byte buffer that is a single contiguous
 * byte array, without resize ability.
 */
final class SimpleByteBuffer
    implements ByteBuffer
{
    byte[]  _bytes;
    int     _start;
    int     _eob;
    boolean _is_read_only;


    /**
     * Creates a read-write buffer.
     *
     * @param bytes assumed to be owned by this new instance.
     */
    public SimpleByteBuffer(byte[] bytes) {
        this(bytes, 0, bytes.length, false);
    }

    /**
     *
     * @param bytes assumed to be owned by this new instance.
     */
    public SimpleByteBuffer(byte[] bytes, boolean isReadOnly) {
        this(bytes, 0, bytes.length, isReadOnly);
    }

    /**
     * Creates a read-write buffer.
     *
     * @param bytes assumed to be owned by this new instance.
     */
    public SimpleByteBuffer(byte[] bytes, int start, int length) {
        this(bytes, start, length, false);
    }

    /**
     *
     * @param bytes assumed to be owned by this new instance.
     */
    public SimpleByteBuffer(byte[] bytes, int start, int length, boolean isReadOnly) {
        if (bytes == null || start < 0 || start > bytes.length || length < 0 || start + length > bytes.length) {
            throw new IllegalArgumentException();
        }
        _bytes = bytes;
        _start = start;
        _eob   = start + length;
        _is_read_only = isReadOnly;
    }

    public int getLength()
    {
        int length = _eob - _start;
        return length;
    }

    /**
     * Makes a copy of the internal byte array.
     */
    public byte[] getBytes()
    {
        int length = _eob - _start;
        byte[] copy = new byte[length];
        System.arraycopy(_bytes, _start, copy, 0, length);
        return copy;
    }

    public int getBytes(byte[] buffer, int offset, int length)
    {
        if (buffer == null || offset < 0 || offset > buffer.length || length < 0 || offset + length > buffer.length) {
            throw new IllegalArgumentException();
        }

        int datalength = _eob - _start;
        if (datalength > length) {
            throw new IllegalArgumentException("insufficient space in destination buffer");
        }

        System.arraycopy(_bytes, _start, buffer, offset, datalength);

        return datalength;

    }
    public ByteReader getReader()
    {
        ByteReader reader = new SimpleByteReader(this);
        return reader;
    }

    public ByteWriter getWriter()
    {
        if (_is_read_only) {
            throw new IllegalStateException("this buffer is read only");
        }
        SimpleByteWriter writer = new SimpleByteWriter(this);
        return writer;
    }

    public void writeBytes(OutputStream out) throws IOException
    {
        int length = _eob - _start;
        out.write(_bytes, _start, length);
    }

    static final class SimpleByteReader implements ByteReader
    {
        SimpleByteBuffer _buffer;
        int              _position;

        SimpleByteReader(SimpleByteBuffer bytebuffer) {
            _buffer = bytebuffer;
            _position = bytebuffer._start;
        }

        public int position()
        {
            int pos = _position - _buffer._start;
            return pos;
        }

        public void position(int newPosition)
        {
            if (newPosition < 0) {
                throw new IllegalArgumentException("position must be non-negative");
            }
            int pos = newPosition + _buffer._start;
            if (pos > _buffer._eob) {
               throw new IllegalArgumentException("position is past end of buffer");
            }
            _position = pos;
        }

        public void skip(int length)
        {
            if (length < 0) {
                throw new IllegalArgumentException("length to skip must be non-negative");
            }
            int pos = _position + length;
            if (pos > _buffer._eob) {
               throw new IllegalArgumentException("skip would skip past end of buffer");
            }
            _position = pos;
        }

        public int read()
        {
            if (_position >= _buffer._eob) return EOF;
            int b = _buffer._bytes[_position++];
            return (b & 0xff);
        }

        public int read(byte[] dst, int start, int len)
        {
            if (dst == null || start < 0 || len < 0 || start + len > dst.length) {
                // no need to test this start >= dst.length ||
                // since we test start+len > dst.length which is the correct test
                throw new IllegalArgumentException();
            }

            if (_position >= _buffer._eob) return 0;
            int readlen = len;
            if (readlen + _position > _buffer._eob) readlen = _buffer._eob - _position;
            System.arraycopy(_buffer._bytes, _position, dst, start, readlen);
            _position += readlen;
            return readlen;
        }

        public int readTypeDesc()
        {
            return read();
        }

        public long readULong(int len) throws IOException
        {
            long    retvalue = 0;
            int b;

            switch (len) {
            default:
                throw new IonException("value too large for Java long");
            case 8:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 7:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 6:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 5:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 4:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 3:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 2:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 1:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 8) | b;
            case 0:
                // do nothing, it's just a 0 length is a 0 value
            }
            return retvalue;
        }


        public int readVarInt() throws IOException
        {
            int     retvalue = 0;
            boolean is_negative = false;
            int     b;

            // synthetic label "done" (yuck)
done:       for (;;) {
                // read the first byte - it has the sign bit
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((b & 0x40) != 0) {
                    is_negative = true;
                }
                retvalue = (b & 0x3F);
                if ((b & 0x80) != 0) break done;

                // for the second byte we shift our eariler bits just as much,
                // but there are fewer of them there to shift
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // if we get here we have more bits than we have room for :(
                throwIntOverflowExeption();
            }
            if (is_negative) {
                retvalue = -retvalue;
            }
            return retvalue;
        }

        public long readVarLong() throws IOException
        {
            long    retvalue = 0;
            boolean is_negative = false;
            int     b;

            // synthetic label "done" (yuck)
done:       for (;;) {
                // read the first byte - it has the sign bit
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((b & 0x40) != 0) {
                    is_negative = true;
                }
                retvalue = (b & 0x3F);
                if ((b & 0x80) != 0) break done;

                // for the second byte we shift our eariler bits just as much,
                // but there are fewer of them there to shift
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                for (;;) {
                    if ((b = read()) < 0) throwUnexpectedEOFException();
                    if ((retvalue & 0xFE00000000000000L) != 0) throwIntOverflowExeption();
                    retvalue = (retvalue << 7) | (b & 0x7F);
                    if ((b & 0x80) != 0) break done;
                }
            }
            if (is_negative) {
                retvalue = -retvalue;
            }
            return retvalue;
        }

        /**
         * Reads an integer value, returning null to mean -0.
         * @throws IOException
         */
        public Integer readVarInteger() throws IOException
        {
            int     retvalue = 0;
            boolean is_negative = false;
            int     b;

            // sythetic label "done" (yuck)
done:       for (;;) {
                // read the first byte - it has the sign bit
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((b & 0x40) != 0) {
                    is_negative = true;
                }
                retvalue = (b & 0x3F);
                if ((b & 0x80) != 0) break done;

                // for the second byte we shift our eariler bits just as much,
                // but there are fewer of them there to shift
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // if we get here we have more bits than we have room for :(
                throwIntOverflowExeption();
            }

            Integer retInteger = null;
            if (is_negative) {
                if (retvalue != 0) {
                    retInteger = Integer.valueOf(-retvalue);
                }
            }
            else {
                retInteger = Integer.valueOf(retvalue);
            }
            return retInteger;
        }

        public int readVarUInt() throws IOException
        {
            int retvalue = 0;
            int  b;

            for (;;) { // fake loop to create a "goto done"
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;

                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;

                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;

                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;

                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;

                // if we get here we have more bits than we have room for :(
                throwIntOverflowExeption();
            }
            return retvalue;
        }

        public double readFloat(int len) throws IOException
        {
            if (len == 0)
            {
                // special case, return pos zero
                return 0.0d;
            }

            if (len != 8)
            {
                throw new IOException("Length of float read must be 0 or 8");
            }

            long dBits = this.readULong(len);
            return Double.longBitsToDouble(dBits);
        }

        public long readVarULong() throws IOException
        {
            long retvalue = 0;
            int  b;

            for (;;) {
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((retvalue & 0xFE00000000000000L) != 0) throwIntOverflowExeption();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;
            }
            return retvalue;
        }

        /**
         * Near clone of {@link IonReaderBinaryRawX#readDecimal(int)}
         * and {@link IonBinary.Reader#readDecimalValue(IonDecimalImpl, int)}
         * so keep them in sync!
         */
        public Decimal readDecimal(int len) throws IOException
        {
            MathContext mathContext = MathContext.UNLIMITED;

            Decimal bd;

            // we only write out the '0' value as the nibble 0
            if (len == 0) {
                bd = Decimal.valueOf(0, mathContext);
            }
            else {
                // otherwise we to it the hard way ....
                int         startpos = this.position();
                int         exponent = this.readVarInt();
                int         bitlen = len - (this.position() - startpos);

                BigInteger value;
                int signum;
                if (bitlen > 0)
                {
                    byte[] bits = new byte[bitlen];
                    this.read(bits, 0, bitlen);

                    signum = 1;
                    if (bits[0] < 0)
                    {
                        // value is negative, clear the sign
                        bits[0] &= 0x7F;
                        signum = -1;
                    }
                    value = new BigInteger(signum, bits);
                }
                else {
                    signum = 0;
                    value = BigInteger.ZERO;
                }

                // Ion stores exponent, BigDecimal uses the negation "scale"
                int scale = -exponent;
                if (value.signum() == 0 && signum == -1)
                {
                    assert value.equals(BigInteger.ZERO);
                    bd = Decimal.negativeZero(scale, mathContext);
                }
                else
                {
                    bd = Decimal.valueOf(value, scale, mathContext);
                }
            }
            return bd;
        }

        public Timestamp readTimestamp(int len) throws IOException
        {
            if (len < 1) {
                // nothing to do here - and the timestamp will be NULL
                return null;
            }

            Timestamp val;
            Precision   p = null; // FIXME remove
            Integer     offset = null;
            int         year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
            BigDecimal  frac = null;
            int         remaining, end = this.position() + len;

            // first up is the offset, which requires a special int reader
            // to return the -0 as a null Integer
            offset = this.readVarInteger();

            // now we'll read the struct values from the input stream
            assert position() < end;
            if (position() < end) {  // FIXME remove
                // year is from 0001 to 9999
                // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
                year  = readVarUInt();
                p = Precision.YEAR; // our lowest significant option

                // now we look for hours and minutes
                if (position() < end) {
                    month = readVarUInt();
                    p = Precision.MONTH;

                    // now we look for hours and minutes
                    if (position() < end) {
                        day   = readVarUInt();
                        p = Precision.DAY; // our lowest significant option

                        // now we look for hours and minutes
                        if (position() < end) {
                            hour   = readVarUInt();
                            minute = readVarUInt();
                            p = Precision.MINUTE;

                            if (position() < end) {
                                second = readVarUInt();
                                p = Precision.SECOND;

                                remaining = end - position();
                                if (remaining > 0) {
                                    // now we read in our actual "milliseconds since the epoch"
                                    frac = this.readDecimal(remaining);
                                }
                            }
                        }
                    }
                }
            }

            // now we let timestamp put it all together
            val = Timestamp.createFromUtcFields(p, year, month, day, hour, minute, second, frac, offset);
            return val;
        }

        public String readString(int len) throws IOException
        {
            // len is bytes, which is greater than or equal to java
            // chars even after utf8 to utf16 decoding nonsense
            // the char array is way faster than using string buffer
            char[] chars = new char[len];
            int ii = 0;
            int c;
            int endPosition = this.position() + len;

            while (this.position() < endPosition) {
                c = readUnicodeScalar();
                if (c < 0) throwUnexpectedEOFException();
                if (c < 0x10000) {
                    chars[ii++] = (char)c;
                }
                else { // when c is >= 0x10000 we need surrogate encoding
                    chars[ii++] = (char)_Private_IonConstants.makeHighSurrogate(c);
                    chars[ii++] = (char)_Private_IonConstants.makeLowSurrogate(c);
                }
            }
            if (this.position() < endPosition) throwUnexpectedEOFException();

            return new String(chars, 0, ii);
        }

        public final int readUnicodeScalar() throws IOException {
            int b;

            b = read();
            // ascii is all good, even -1 (eof)
            if (b >= 0x80) {
                b = readUnicodeScalar_helper(b);

            }
            return b;
        }
        private final  int readUnicodeScalar_helper(int b) throws IOException
        {
            int c = -1;

            // now we start gluing the multi-byte value together
            if ((b & 0xe0) == 0xc0) {
                // for values from 0x80 to 0x7FF (all legal)
                c = (b & ~0xe0);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
            }
            else if ((b & 0xf0) == 0xe0) {
                // for values from 0x800 to 0xFFFFF (NOT all legal)
                c = (b & ~0xf0);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                if (c > 0x00D7FF && c < 0x00E000) {
                    throw new IonException("illegal surrogate value encountered in input utf-8 stream");
                }
            }
            else if ((b & 0xf8) == 0xf0) {
                // for values from 0x010000 to 0x1FFFFF (NOT all legal)
                c = (b & ~0xf8);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                if (c > 0x10FFFF) {
                    throw new IonException("illegal utf value encountered in input utf-8 stream");
                }
            }
            else {
                throwUTF8Exception();
            }
            return c;
        }
        void throwUTF8Exception() throws IOException
        {
            throw new IOException("Invalid UTF-8 character encounter in a string at pos " + this.position());
        }
        void throwUnexpectedEOFException() throws IOException {
            throw new IOException("unexpected EOF in value at offset " + this.position());
        }
        void throwIntOverflowExeption() throws IOException {
            throw new IOException("int in stream is too long for a Java int 32 use readLong()");
        }
    }

    static final class UserByteWriter extends OutputStream implements ByteWriter
    {
        SimpleByteWriter _simple_writer;
        OutputStream     _user_stream;
        int              _position;
        int              _limit;
        int              _buffer_size;

        // constants used for the various write routines to verify
        // sufficient room in the output buffer for writing
        private final static int MAX_UINT7_BINARY_LENGTH = 5; // (1 + (32 / 7))
        private final static int MAX_FLOAT_BINARY_LENGTH = 8; // ieee 64 bit float

        private final static int REQUIRED_BUFFER_SPACE   = 8; // max of the required lengths

        UserByteWriter(OutputStream userOuputStream, byte[] buf)
        {
            if (buf == null || buf.length < REQUIRED_BUFFER_SPACE) {
                throw new IllegalArgumentException("requires a buffer at least "+REQUIRED_BUFFER_SPACE+" bytes long");
            }

            SimpleByteBuffer bytebuffer = new SimpleByteBuffer(buf);
            _simple_writer =  new SimpleByteWriter(bytebuffer);
            _user_stream = userOuputStream;
            _buffer_size = buf.length;
            _limit = _buffer_size;
        }

        private final void checkForSpace(int needed) {
            if (_position + needed > _limit) {
                flush();
            }
        }

        /**
         * this flushes the current buffer to the users output
         * stream and resets the buffer.  It is called automatically
         * as the buffer fills.  It can be called at any time if more
         * frequent flushing is desired.
         */
        @Override
        public void flush() {
            // is there anything to flush?
            if (_position + _buffer_size > _limit) {
                try {
                    _simple_writer.flushTo(_user_stream);
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
                _limit = _position + _buffer_size;
            }
        }

        public void insert(int length)
        {
            throw new UnsupportedOperationException("use a SimpleByteWriter if you need to insert");
        }

        public int position()
        {
            return _position;
        }

        public void position(int newPosition)
        {
            throw new UnsupportedOperationException("use a SimpleByteWriter if you need to set your position");
        }

        public void remove(int length)
        {
            throw new UnsupportedOperationException("use a SimpleByteWriter if you need to remove bytes");
        }

        @Override
        public void write(int b) throws IOException
        {
            write((byte)b);
        }

        public void write(byte b) throws IOException
        {
            checkForSpace(1);
            _simple_writer.write(b);
            _position++;
        }

        // cloned from SimpleByteBuffer as we need to check the
        // length and this may require flushing more often than
        // the default writeDecimal supports.
        public int writeDecimal(BigDecimal value) throws IOException
        {
            int returnlen = _simple_writer.writeDecimal(value, this);
            return returnlen;
        }

        public int writeFloat(double value) throws IOException
        {
            checkForSpace(MAX_FLOAT_BINARY_LENGTH);
            int returnlen = _simple_writer.writeFloat(value);
            return returnlen;
        }

        public int writeIonInt(long value, int len) throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeIonInt(value, len);
            return returnlen;
        }

        public int writeIonInt(int value, int len) throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeIonInt(value, len);
            return returnlen;
        }

        public int writeString(String value) throws IOException
        {
            int returnlen = _simple_writer.writeString(value, this);
            return returnlen;
        }

        public void writeTypeDesc(int typeDescByte) throws IOException
        {
            checkForSpace(1);
            _simple_writer.writeTypeDesc(typeDescByte);
        }

        public int writeTypeDescWithLength(int typeid, int lenOfLength,
                                           int valueLength) throws IOException
        {
            checkForSpace(1 + MAX_UINT7_BINARY_LENGTH);
            int returnlen = _simple_writer.writeTypeDescWithLength(typeid, lenOfLength, valueLength);
            return returnlen;
        }

        public int writeTypeDescWithLength(int typeid, int valueLength) throws IOException
        {
            checkForSpace(1 + MAX_UINT7_BINARY_LENGTH);
            int returnlen = _simple_writer.writeTypeDescWithLength(typeid, valueLength);
            return returnlen;
        }

        public int writeVarInt(long value, int len, boolean forceZeroWrite)
            throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeVarInt(value, len, forceZeroWrite);
            return returnlen;
        }

        public int writeVarInt(int value, int len, boolean forceZeroWrite)
            throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeVarInt(value, len, forceZeroWrite);
            return returnlen;
        }

        public int writeVarUInt(int value, int len, boolean forceZeroWrite)
            throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeVarUInt(value, len, forceZeroWrite);
            return returnlen;
        }

        public int writeVarUInt(long value, int len, boolean forceZeroWrite)
            throws IOException
        {
            checkForSpace(len);
            int returnlen = _simple_writer.writeVarUInt(value, len, forceZeroWrite);
            return returnlen;
        }
    }

    static final class SimpleByteWriter extends OutputStream implements ByteWriter
    {
        private static final int _ib_FLOAT64_LEN         =    8;
        private static final Double DOUBLE_POS_ZERO = Double.valueOf(0.0);

        SimpleByteBuffer _buffer;
        int              _position;

        SimpleByteWriter(SimpleByteBuffer bytebuffer) {
            _buffer = bytebuffer;
            _position = bytebuffer._start;
        }

        protected void flushTo(OutputStream userOutput) throws IOException {
            _buffer.writeBytes(userOutput);
            _position = 0;
        }

        public int position()
        {
            return _position - _buffer._start;
        }

        public void position(int newPosition)
        {
            if (newPosition < 0) {
                throw new IllegalArgumentException("position must be non-negative");
            }
            int pos = newPosition + _buffer._start;
            if (pos > _buffer._eob) {
               throw new IllegalArgumentException("position is past end of buffer");
            }
            _position = pos;
        }

        public void insert(int length)
        {
            if (length < 0) {
                throw new IllegalArgumentException("insert length must be non negative");
            }
            int remaining = _buffer._eob - _position;
            System.arraycopy(_buffer._bytes, _position, _buffer._bytes, _position + length, remaining);
            _buffer._eob += length;
        }

        public void remove(int length)
        {
            if (length < 0) {
                throw new IllegalArgumentException("remove length must be non negative");
            }
            int remaining = _buffer._eob - _position;
            System.arraycopy(_buffer._bytes, _position + length, _buffer._bytes, _position, remaining);
            _buffer._eob -= length;
        }

        @Override
        final public void write(int arg0)
            throws IOException
        {
            write((byte)arg0);
        }

        final public void write(byte b)
        {
            _buffer._bytes[_position++] = b;
            if (_position > _buffer._eob) _buffer._eob = _position;
        }

        @Override
        public void write(byte[] bytes, int start, int len)
        {
            if (bytes == null || start < 0 || start >= bytes.length || len < 0 || start + len > bytes.length) {
                throw new IllegalArgumentException();
            }

            System.arraycopy(bytes, start, _buffer._bytes, _position, len);
            _position += len;
            if (_position > _buffer._eob) _buffer._eob = _position;

            return;
        }

        public void writeTypeDesc(int typeDescByte)
        {
            write((byte)(typeDescByte & 0xff));
        }

        public int writeTypeDescWithLength(int typeid, int lenOfLength, int valueLength)
        {
            int written_len = 1;

            int td = ((typeid & 0xf) << 4);
            if (valueLength >= _Private_IonConstants.lnIsVarLen) {
                td |= _Private_IonConstants.lnIsVarLen;
                writeTypeDesc(td);
                written_len += writeVarUInt(valueLength, lenOfLength, true);
            }
            else {
                td |= (valueLength & 0xf);
                writeTypeDesc(td);
            }
            return written_len;
        }

        public int writeTypeDescWithLength(int typeid, int valueLength)
        {
            int written_len = 1;

            int td = ((typeid & 0xf) << 4);
            if (valueLength >= _Private_IonConstants.lnIsVarLen) {
                td |= _Private_IonConstants.lnIsVarLen;
                writeTypeDesc(td);
                int lenOfLength = IonBinary.lenVarUInt(valueLength);
                written_len += writeVarUInt(valueLength, lenOfLength, true);
            }
            else {
                td |= (valueLength & 0xf);
                writeTypeDesc(td);
            }
            return written_len;
        }

        public int writeVarInt(int value, int len, boolean force_zero_write)
        {
            // int len = 0;

            if (value != 0) {
                int mask = 0x7F;
                boolean is_negative = false;

                assert len == IonBinary.lenVarInt(value);
                if (is_negative = (value < 0)) {
                    value = -value;
                }

                // we write the first "byte" separately as it has the sign
                // changed shift operator from >> to >>>, we don't want to
                // sign extend, this is only a problem with MIN_VALUE where
                // negating the value (above) still results in a negative number
                int b = (byte)((value >>> (7*(len-1))) & mask);
                if (is_negative)  b |= 0x40; // the sign bit only on the first byte
                if (len == 1)     b |= 0x80; // the terminator in case the first "byte" is the last
                write((byte)b);

                // write the rest
                switch (len) {  // we already wrote 1 byte
                case 5: write((byte)((value >> (7*3)) & mask));
                case 4: write((byte)((value >> (7*2)) & mask));
                case 3: write((byte)((value >> (7*1)) & mask));
                case 2: write((byte)((value & mask) | 0x80));
                case 1: // do nothing
                }
            }
            else if (force_zero_write) {
                write((byte)0x80);
                assert len == 1;

            }
            else {
                assert len == 0;
            }
            return len;
        }

        public int writeVarInt(int value, boolean force_zero_write)
        {
            int len = IonBinary.lenVarInt(value);
            len = writeVarInt(value, len, force_zero_write);
            return len;
        }

        public int writeVarUInt(int value, int len, boolean force_zero_write)
        {
            int mask = 0x7F;
            if (value < 0) {
                throw new IllegalArgumentException("signed int where unsigned (>= 0) was expected");
            }
            assert len == IonBinary.lenVarUInt(value);

            switch (len - 1) {
            case 4: write((byte)((value >> (7*4)) & mask));
            case 3: write((byte)((value >> (7*3)) & mask));
            case 2: write((byte)((value >> (7*2)) & mask));
            case 1: write((byte)((value >> (7*1)) & mask));
            case 0: write((byte)((value & mask) | 0x80L));
                    break;
            case -1: // or 0
                if (force_zero_write) {
                    write((byte)0x80);
                    len = 1;
                }
                break;
            }
            return len;
        }

        public int writeVarUInt(int value, boolean force_zero_write)
        {
            int len = IonBinary.lenVarUInt(value);
            len = writeVarUInt(value, len, force_zero_write);
            return len;
        }

        public int writeIonInt(int value, int len)
        {
            return writeIonInt((long)value, len);
        }

        public int writeIonInt(long value, int len)
        {
            // we shouldn't be writing out 0's as an Ion int value
            if (value == 0) {
                assert len == 0;
                return len;  // aka 0
            }

            // figure out how many we have bytes we have to write out
            long mask = 0xffL;
            boolean is_negative = (value < 0);

            assert len == IonBinary.lenIonInt(value);

            if (is_negative) {
                value = -value;
                // note for Long.MIN_VALUE the negation returns
                // itself as a value, but that's also the correct
                // "positive" value to write out anyway, so it
                // all works out
            }

            // write the rest
            switch (len) {  // we already wrote 1 byte
            case 8: write((byte)((value >> (8*7)) & mask));
            case 7: write((byte)((value >> (8*6)) & mask));
            case 6: write((byte)((value >> (8*5)) & mask));
            case 5: write((byte)((value >> (8*4)) & mask));
            case 4: write((byte)((value >> (8*3)) & mask));
            case 3: write((byte)((value >> (8*2)) & mask));
            case 2: write((byte)((value >> (8*1)) & mask));
            case 1: write((byte)(value & mask));
            }

            return len;
        }

        public int writeVarInt(long value, int len, boolean forceZeroWrite)
        {
            //int len = 0;

            if (value != 0) {
                long mask = 0x7fL;
                assert len == IonBinary.lenInt(value);
                int b;
                if (value < 0) {
                    value = -value;
                    // we write the first "byte" separately as it has the sign
                    // and we have to deal with the oddball MIN_VALUE case
                    if (value == Long.MIN_VALUE) {
                        // we use the shift without sign extension as this
                        // represents a positive value (even though it's neg)
                        b = (byte)((value >>> (7*len)) & mask);
                        // len must be greater than 1 so we don't need to set the high bit
                    }
                    else {
                        // here, and hereafter, we don't care about sign extension
                        b = (byte)((value >>> (7*len)) & mask);
                        if (len == 1) b |= 0x80; // the terminator in case the first "byte" is the last
                    }
                    // we don't worry about stepping on a data bit here as
                    // we have extra bits at the max size and below that we've
                    // already taken the sign bit into account
                    b |= 0x40; // the sign bit
                    write((byte)b);
                }
                else {
                    // we write the first "byte" separately as it has the sign
                    b = (byte)((value >>> (7*len)) & mask);
                    if (len == 1) b |= 0x80; // the terminator in case the first "byte" is the last
                    write((byte)b);
                }

                // write the rest
                switch (len - 1) {  // we already wrote 1 byte
                case 9: write((byte)((value >>> (7*9)) & mask));
                case 8: write((byte)((value >> (7*8)) & mask));
                case 7: write((byte)((value >> (7*7)) & mask));
                case 6: write((byte)((value >> (7*6)) & mask));
                case 5: write((byte)((value >> (7*5)) & mask));
                case 4: write((byte)((value >> (7*4)) & mask));
                case 3: write((byte)((value >> (7*3)) & mask));
                case 2: write((byte)((value >> (7*2)) & mask));
                case 1: write((byte)((value >> (7*1)) & mask));
                case 0: write((byte)((value & mask) | 0x80L));
                }
            }
            else if (forceZeroWrite) {
                write((byte)0x80);
                assert len == 1;
            }
            else {
                assert len == 0;
            }
            return len;
        }

        public int writeVarUInt(long value, int len, boolean force_zero_write)
        {
            int mask = 0x7F;
            assert len == IonBinary.lenVarUInt(value);
            assert value > 0;

            switch (len - 1) {
            case 9: write((byte)((value >> (7*9)) & mask));
            case 8: write((byte)((value >> (7*8)) & mask));
            case 7: write((byte)((value >> (7*7)) & mask));
            case 6: write((byte)((value >> (7*6)) & mask));
            case 5: write((byte)((value >> (7*5)) & mask));
            case 4: write((byte)((value >> (7*4)) & mask));
            case 3: write((byte)((value >> (7*3)) & mask));
            case 2: write((byte)((value >> (7*2)) & mask));
            case 1: write((byte)((value >> (7*1)) & mask));
            case 0: write((byte)((value & mask) | 0x80L));
                    break;
            case -1: // or len == 0
                if (force_zero_write) {
                    write((byte)0x80);
                    assert len == 1;
                }
                else {
                    assert len == 0;
                }
                break;
            }
            return len;
        }

        public int writeULong(long value, int lenToWrite) throws IOException
        {
            switch (lenToWrite) {
            case 8: write((byte)((value >> 56) & 0xffL));
            case 7: write((byte)((value >> 48) & 0xffL));
            case 6: write((byte)((value >> 40) & 0xffL));
            case 5: write((byte)((value >> 32) & 0xffL));
            case 4: write((byte)((value >> 24) & 0xffL));
            case 3: write((byte)((value >> 16) & 0xffL));
            case 2: write((byte)((value >>  8) & 0xffL));
            case 1: write((byte)((value >>  0) & 0xffL));
            }
            return lenToWrite;
        }

        public int writeFloat(double value) throws IOException
        {
            if (Double.valueOf(value).equals(DOUBLE_POS_ZERO))
            {
                // pos zero special case
                return 0;
            }

            // TODO write "custom" serialization or verify that
            //      the java routine is doing the right thing
            long dBits = Double.doubleToRawLongBits(value);
            return this.writeULong(dBits, _ib_FLOAT64_LEN);
        }

        public int writeDecimal(BigDecimal value) throws IOException
        {
            int returnlen = writeDecimal(value, null);
            return returnlen;
        }

        // this is a private version that supports both the buffer write
        // and the output to the user stream.  This is slower (since there
        // are extra tests, an extra call, and an extra paramenter), but
        // it avoids duplicating this code.  And decimals are generally
        // expensive anyway.
        private int writeDecimal(BigDecimal value, UserByteWriter userWriter) throws IOException
        {
            int returnlen = 0;
            // we only write out the '0' value as the nibble 0
            if (value != null && !BigDecimal.ZERO.equals(value)) {
                // otherwise we do it the hard way ....
                BigInteger mantissa = value.unscaledValue();

                boolean isNegative = (mantissa.compareTo(BigInteger.ZERO) < 0);
                if (isNegative) {
                    mantissa = mantissa.negate();
                }

                byte[] bits  = mantissa.toByteArray();
                int    scale = value.scale();

                // Ion stores exponent, BigDecimal uses the negation "scale"
                int exponent = -scale;
                if (userWriter != null) {
                    returnlen += userWriter.writeIonInt(exponent, IonBinary.lenVarUInt(exponent));
                }
                else {
                    returnlen += this.writeIonInt(exponent, IonBinary.lenVarUInt(exponent));
                }

                // If the first bit is set, we can't use it for the sign,
                // and we need to write an extra byte to hold it.
                boolean needExtraByteForSign = ((bits[0] & 0x80) != 0);
                if (needExtraByteForSign)
                {
                    if (userWriter != null) {
                        userWriter.write((byte)(isNegative ? 0x80 : 0x00));
                    }
                    else {
                        this.write((byte)(isNegative ? 0x80 : 0x00));
                    }
                    returnlen++;
                }
                else if (isNegative)
                {
                    bits[0] |= 0x80;
                }
                // if we have a userWriter to write to, we really don't care about
                // the value in our local buffer.
                if (userWriter != null) {
                    userWriter.write(bits, 0, bits.length);
                }
                else {
                    this.write(bits, 0, bits.length);
                }
                returnlen += bits.length;
            }
            return returnlen;
        }

        public int writeTimestamp(Timestamp di)
        throws IOException
        {
            if (di == null) return 0;

            int returnlen = 0;
            Precision precision = di.getPrecision();

            Integer offset = di.getLocalOffset();
            if (offset == null) {
                // TODO don't use magic numbers!
                this.write((byte)(0xff & (0x80 | 0x40))); // negative 0 (no timezone)
                returnlen ++;
            }
            else {
                int value = offset.intValue();
                returnlen += this.writeVarInt(value, true);
            }

            // now the date - year, month, day as varUint7's
            // if we have a non-null value we have at least the date
            if (precision.includes(Precision.YEAR)) {
                returnlen += this.writeVarUInt(di.getZYear(), true);
            }
            if (precision.includes(Precision.MONTH)) {
                returnlen += this.writeVarUInt(di.getZMonth(), true);
            }
            if (precision.includes(Precision.DAY)) {
                returnlen += this.writeVarUInt(di.getZDay(), true);
            }

            // now the time part
            if (precision.includes(Precision.MINUTE)) {
                returnlen += this.writeVarUInt(di.getZHour(), true);
                returnlen += this.writeVarUInt(di.getZMinute(), true);
            }
            if (precision.includes(Precision.SECOND)) {
                returnlen += this.writeVarUInt(di.getZSecond(), true);
                BigDecimal fraction = di.getZFractionalSecond();
                if (fraction != null) {
                    // and, finally, any fractional component that is known
                    returnlen += this.writeDecimal(di.getZFractionalSecond());
                }
            }
            return returnlen;
        }

        final public int writeString(String value) throws IOException
        {
            int returnlen = writeString(value, null);
            return returnlen;
        }

        final private int writeString(String value, UserByteWriter userWriter) throws IOException
        {
            int len = 0;

            for (int ii=0; ii<value.length(); ii++) {
                int c = value.charAt(ii);
                if (c > 127) {
                    if (c >= 0xD800 && c <= 0xDFFF) {
                        if (_Private_IonConstants.isHighSurrogate(c)) {
                            ii++;
                            // houston we have a high surrogate (let's hope it has a partner
                            if (ii >= value.length()) {
                                throw new IonException("invalid string, unpaired high surrogate character");
                            }
                            int c2 = value.charAt(ii);
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
                    c = IonBinary.makeUTF8IntFromScalar(c);
                }

                // write it here - try to test userWriter as little as possible
                if (userWriter == null) {
                    for (;;) {
                        write((byte)(c & 0xff));
                        len++;
                        if ((c & 0xffffff00) == 0) {
                            break;
                        }
                        c = c >>> 8;
                    }
                }
                else {
                    for (;;) {
                        userWriter.write((byte)(c & 0xff));
                        len++;
                        if ((c & 0xffffff00) == 0) {
                            break;
                        }
                        c = c >>> 8;
                    }
                }
            }
            return len;
        }
        void throwUTF8Exception() throws IOException
        {
            throwException("Invalid UTF-8 character encounter in a string at pos " + this.position());
        }
        void throwException(String msg) throws IOException {
            throw new IOException(msg);
        }
    }
}
