/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import com.amazon.ion.TtTimestamp;

import com.amazon.ion.IonException;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonConstants;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

/**
 * Manages a very simple byte buffer that is a single contiguous
 * byte array, without resize ability.
 */
public final class SimpleByteBuffer
    implements ByteBuffer
{
    byte[]  _bytes;
    int     _start;
    int     _eob;
    boolean _is_read_only;


    public SimpleByteBuffer(byte[] bytes) {
        this(bytes, 0, bytes.length, false);
    }
    public SimpleByteBuffer(byte[] bytes, boolean isReadOnly) {
        this(bytes, 0, bytes.length, isReadOnly);
    }
    public SimpleByteBuffer(byte[] bytes, int start, int length) {
        this(bytes, start, length, !(start == 0 && start + length == bytes.length));
    }
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
                    retInteger = new Integer(-retvalue);
                }
            }
            else {
                retInteger = new Integer(retvalue);
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

        public BigDecimal readDecimal(int len) throws IOException
        {
            BigDecimal bd;

            // we only write out the '0' value as the nibble 0
            if (len == 0) {
                bd = new BigDecimal(0, MathContext.DECIMAL128);
            }
            else {
                // otherwise we to it the hard way ....
                int         startpos = this.position();
                int         exponent = this.readVarInt();
                int         bitlen = len - (this.position() - startpos);

                BigInteger value;
                if (bitlen > 0)
                {
                    byte[] bits = new byte[bitlen];
                    this.read(bits, 0, bitlen);

                    int signum = 1;
                    if (bits[0] < 0)
                    {
                        // value is negative, clear the sign
                        bits[0] &= 0x7F;
                        signum = -1;
                    }
                    value = new BigInteger(signum, bits);
                }
                else {
                    value = BigInteger.ZERO;
                }

                // Ion stores exponent, BigDecimal uses the negation "scale"
                int scale = -exponent;
                bd = new BigDecimal(value, scale, MathContext.DECIMAL128);
            }
            return bd;
        }

        public TtTimestamp readTimestamp(int len) throws IOException
        {
            if (len < 1) return null;
            int startpos = this.position();

            // first the timezone, and -0 is unknown
            Integer tz = this.readVarInteger();

            // now the time part
            BigDecimal bd = this.readDecimal(len - (this.position() - startpos));

            // now we put it together
            return new TtTimestamp(bd, tz);
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
                    chars[ii++] = (char)IonConstants.makeHighSurrogate(c);
                    chars[ii++] = (char)IonConstants.makeLowSurrogate(c);
                }
            }
            if (this.position() < endPosition) throwUnexpectedEOFException();

            return new String(chars, 0, ii);
        }

        public int readUnicodeScalar() throws IOException {
            int c = -1, b;

            b = read();
            // ascii is all good, even -1 (eof)
            if (b < 0x80) {
                return b;
            }

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
            if (valueLength >= IonConstants.lnIsVarLen) {
                td |= IonConstants.lnIsVarLen;
                writeTypeDesc(td);
                written_len += writeVarUInt(valueLength, lenOfLength, true);
            }
            else {
                td |= (valueLength & 0xf);
                writeTypeDesc(td);
            }
            return written_len;
        }

        public int writeTypeDescWithLength2(int typeid, int valueLength)
        {
            int written_len = 1;

            int td = ((typeid & 0xf) << 4);
            if (valueLength >= IonConstants.lnIsVarLen) {
                td |= IonConstants.lnIsVarLen;
                writeTypeDesc(td);
                int lenOfLength = IonBinary.lenVarUInt7(valueLength);
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

                assert len == IonBinary.lenVarInt7(value);
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

        public int writeVarUInt(int value, int len, boolean force_zero_write)
        {
            int mask = 0x7F;
            if (value < 0) {
                throw new IllegalArgumentException("signed int where unsigned (>= 0) was expected");
            }
            assert len == IonBinary.lenVarUInt7(value);

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

        public int writeVarInt(long value, int len, boolean force_zero_write)
        {
            //int len = 0;

            if (value != 0) {
                long mask = 0x7fL;
                assert len == IonBinary.lenVarInt8(value);
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
            else if (force_zero_write) {
                write((byte)0x80);
                assert len == 1;
            }
            else {
                assert len == 0;
            }
            return len;
        }

        public int writeVarUint(long value, int len, boolean force_zero_write)
        {
            int mask = 0x7F;
            assert len == IonBinary.lenVarUInt7(value);
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

        public int writeDecimal(BigDecimal value)
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
                returnlen += this.writeIonInt(exponent, IonBinary.lenVarUInt7(exponent));

                // If the first bit is set, we can't use it for the sign,
                // and we need to write an extra byte to hold it.
                boolean needExtraByteForSign = ((bits[0] & 0x80) != 0);
                if (needExtraByteForSign)
                {
                    this.write((byte)(isNegative ? 0x80 : 0x00));
                    returnlen++;
                }
                else if (isNegative)
                {
                    bits[0] |= 0x80;
                }
                this.write(bits, 0, bits.length);
                returnlen += bits.length;
            }
            return returnlen;
        }

        public int writeTimestamp(TtTimestamp di)
        throws IOException
        {
            int  returnlen = 0;

            if (di != null) {
                long l = di.getMillis();
                BigDecimal bd = new BigDecimal(l);
                bd.setScale(13); // millisecond time has 13 significant digits

                Integer localOffset = di.getLocalOffset();
                int  tzoffset = (localOffset == null) ? 0 : localOffset.intValue();

                int  tzlen = IonBinary.lenVarInt7(tzoffset);
                if (tzlen == 0) tzlen = 1;

                if (localOffset == null) {
                    // TODO don't use magic numbers!
                    this.write((byte)(0xff & (0x80 | 0x40))); // negative 0 (no timezone)
                    returnlen ++;
                }
                else {
                    returnlen += writeIonInt(tzoffset, IonBinary.lenVarUInt7(tzoffset));
                }
                returnlen += writeDecimal(bd);
            }
            return returnlen;
        }

        final public int writeString(String value) throws IOException
        {
            int len = 0;

            for (int ii=0; ii<value.length(); ii++) {
                int c = value.charAt(ii);
                if (c < 128) {
                    // don't even both to call the "utf8" converter for ascii
                    write((byte)c);
                    len++;
                }
                else {
                    if (c >= 0xD800) {
                        if (IonConstants.isHighSurrogate(c)) {
                            ii++;
                            // houston we have a high surrogate (let's hope it has a partner
                            if (ii >= value.length()) {
                                throw new IonException("invalid string, unpaired high surrogate character");
                            }
                            int c2 = value.charAt(ii);
                            if (!IonConstants.isLowSurrogate(c2)) {
                                throw new IonException("invalid string, unpaired high surrogate character");
                            }
                            c = IonConstants.makeUnicodeScalar(c, c2);
                        }
                        else if (IonConstants.isLowSurrogate(c)) {
                            // it's a loner low surrogate - that's an error
                            throw new IonException("invalid string, unpaired low surrogate character");
                        }
                        // from 0xE000 up the _writeUnicodeScalar will check for us
                    }
                    len += writeUnicodeScalarAsUTF8(c);
                }
            }
            return len;
        }
        final public int writeUnicodeScalarAsUTF8(int c) throws IOException
        {
            // TO DO: check this encoding, it is from:
            //      http://en.wikipedia.org/wiki/UTF-8
            // we probably should use some sort of Java supported
            // library for this.  this class might be of interest:
            //     CharsetDecoder(Charset cs, float averageCharsPerByte, float maxCharsPerByte)
            // in: java.nio.charset.CharsetDecoder

            int len = -1;

            // first the quick, easy and common case - ascii
            if (c < 0x80) {
                write((byte)(0xff & c ));
                len = 1;
            }
            else if (c < 0x800) {
                // 2 bytes characters from 0x000080 to 0x0007FF
                write((byte)( 0xff & (0xC0 | (c >> 6)) ));
                write((byte)( 0xff & (0x80 | (c & 0x3F)) ));
                len = 2;
            }
            else if (c < 0x10000) {
                // 3 byte characters from 0x800 to 0xFFFF
                // but only 0x800...0xD7FF and 0xE000...0xFFFF are valid
                if (c > 0xD7FF && c < 0xE000) {
                    this.throwUTF8Exception();
                }
                write((byte)( 0xff & (0xE0 |  (c >> 12)) ));
                write((byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
                write((byte)( 0xff & (0x80 |  (c & 0x3F)) ));
                len = 3;
            }
            else if (c <= 0x10FFFF) {
                // 4 byte characters 0x010000 to 0x10FFFF
                // these are are valid
                write((byte)( 0xff & (0xF0 |  (c >> 18)) ));
                write((byte)( 0xff & (0x80 | ((c >> 12) & 0x3F)) ));
                write((byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
                write((byte)( 0xff & (0x80 | (c & 0x3F)) ));
                len = 4;
            }
            else {
                this.throwUTF8Exception();
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
