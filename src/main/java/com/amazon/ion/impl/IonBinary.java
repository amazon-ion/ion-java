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

import static com.amazon.ion.impl._Private_IonConstants.BINARY_VERSION_MARKER_SIZE;
import static com.amazon.ion.impl._Private_Utils.EMPTY_BYTE_ARRAY;
import static com.amazon.ion.impl._Private_Utils.readFully;
import static com.amazon.ion.util.IonStreamUtils.isIonBinary;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.Timestamp.Precision;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl._Private_IonConstants.HighNibble;
import com.amazon.ion.util.IonTextUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Stack;

final class IonBinary
{
    static boolean debugValidation = false;

    private static final BigInteger MAX_LONG_VALUE = new BigInteger(Long.toString(Long.MAX_VALUE));
    private static final int SIZE_OF_LONG = 8;

    final static int _ib_TOKEN_LEN           =    1;
    final static int _ib_VAR_INT32_LEN_MAX   =    5; // 31 bits (java limit) / 7 bits per byte =  5 bytes
    final static int _ib_VAR_INT64_LEN_MAX   =   10; // 63 bits (java limit) / 7 bits per byte = 10 bytes
    final static int _ib_INT64_LEN_MAX       =    8;
    final static int _ib_FLOAT64_LEN         =    8;

    private static final Double DOUBLE_POS_ZERO = Double.valueOf(0.0);

    static final int ZERO_DECIMAL_TYPEDESC =
        _Private_IonConstants.makeTypeDescriptor(_Private_IonConstants.tidDecimal,
            _Private_IonConstants.lnNumericZero);

    static final int NULL_DECIMAL_TYPEDESC =
        _Private_IonConstants.makeTypeDescriptor(_Private_IonConstants.tidDecimal,
            _Private_IonConstants.lnIsNullAtom);

    private IonBinary() { }


    /**
     * Verifies that a reader starts with a valid Ion cookie, throwing an
     * exception if it does not.
     *
     * @param reader must not be null.
     * @throws IonException if there's a problem reading the cookie, or if the
     * data does not start with {@link _Private_IonConstants#BINARY_VERSION_MARKER_1_0}.
     */
    public static void verifyBinaryVersionMarker(Reader reader)
        throws IonException
    {
        try
        {
            int pos = reader.position();
            //reader.sync();
            //reader.setPosition(0);
            byte[] bvm = new byte[BINARY_VERSION_MARKER_SIZE];
            int len = readFully(reader, bvm);
            if (len < BINARY_VERSION_MARKER_SIZE)
            {
                String message =
                    "Binary data is too short: at least " +
                    BINARY_VERSION_MARKER_SIZE +
                    " bytes are required, but only " + len + " were found.";
                throw new IonException(message);

            }

            if (! isIonBinary(bvm))
            {
                StringBuilder buf = new StringBuilder();
                buf.append("Binary data has unrecognized header");
                for (int i = 0; i < bvm.length; i++)
                {
                    int b = bvm[i] & 0xFF;
                    buf.append(" 0x");
                    buf.append(Integer.toHexString(b).toUpperCase());
                }
                throw new IonException(buf.toString());
            }

            //reader.setPosition(0); // cas 19 apr 2008
            reader.setPosition(pos); // cas 5 may 2009 :)
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }

    /* imported from SimpleByteBuffer.SimpleByteWriter */

    static public int writeTypeDescWithLength(OutputStream userstream, int typeid, int lenOfLength, int valueLength) throws IOException
    {
        int written_len = 1;

        int td = ((typeid & 0xf) << 4);
        if (valueLength >= _Private_IonConstants.lnIsVarLen) {
            td |= _Private_IonConstants.lnIsVarLen;
            userstream.write((byte)(td & 0xff));
            written_len += writeVarUInt(userstream, (long)valueLength, lenOfLength, true);
        }
        else {
            td |= (valueLength & 0xf);
            userstream.write((byte)(td & 0xff));
        }
        return written_len;
    }

    static public int writeTypeDescWithLength(OutputStream userstream, int typeid, int valueLength) throws IOException
    {
        int written_len = 1;
        int td = ((typeid & 0xf) << 4);

        if (valueLength >= _Private_IonConstants.lnIsVarLen) {
            td |= _Private_IonConstants.lnIsVarLen;
            userstream.write((byte)(td & 0xff));
            int lenOfLength = IonBinary.lenVarUInt(valueLength);
            written_len += writeVarUInt(userstream, (long)valueLength, lenOfLength, true);
        }
        else {
            td |= (valueLength & 0xf);
            userstream.write((byte)(td & 0xff));
        }
        return written_len;
    }

    static public int writeIonInt(OutputStream userstream, long value, int len) throws IOException
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
        case 8: userstream.write((byte)((value >> (8*7)) & mask));
        case 7: userstream.write((byte)((value >> (8*6)) & mask));
        case 6: userstream.write((byte)((value >> (8*5)) & mask));
        case 5: userstream.write((byte)((value >> (8*4)) & mask));
        case 4: userstream.write((byte)((value >> (8*3)) & mask));
        case 3: userstream.write((byte)((value >> (8*2)) & mask));
        case 2: userstream.write((byte)((value >> (8*1)) & mask));
        case 1: userstream.write((byte)(value & mask));
        }

        return len;
    }

    static public int writeVarUInt(OutputStream userstream, long value)
        throws IOException
    {
        int len = IonBinary.lenVarUInt(value);
        writeVarUInt(userstream, value, len, false);
        return len;
    }

    static public int writeVarUInt(OutputStream userstream, long value, int len,
                                   boolean force_zero_write)
        throws IOException
    {
        int mask = 0x7F;
        assert len == IonBinary.lenVarUInt(value);
        assert value >= 0;

        switch (len - 1) {
        case 9: userstream.write((byte)((value >> (7*9)) & mask));
        case 8: userstream.write((byte)((value >> (7*8)) & mask));
        case 7: userstream.write((byte)((value >> (7*7)) & mask));
        case 6: userstream.write((byte)((value >> (7*6)) & mask));
        case 5: userstream.write((byte)((value >> (7*5)) & mask));
        case 4: userstream.write((byte)((value >> (7*4)) & mask));
        case 3: userstream.write((byte)((value >> (7*3)) & mask));
        case 2: userstream.write((byte)((value >> (7*2)) & mask));
        case 1: userstream.write((byte)((value >> (7*1)) & mask));
        case 0: userstream.write((byte)((value & mask) | 0x80L));
                break;
        case -1: // or len == 0
            if (force_zero_write) {
                userstream.write((byte)0x80);
                assert len == 1;
            }
            else {
                assert len == 0;
            }
            break;
        }
        return len;
    }

    static public int writeString(OutputStream userstream, String value)
        throws IOException
    {
        int len = 0;
        for (int ii=0; ii<value.length(); ii++) {
            int c = value.charAt(ii);
            if (c < 128) {
                userstream.write((byte)c);
                ++len;
                continue;
            }
            // multi-byte utf8
            if (c >= 0xD800 && c <= 0xDFFF) {
                if (_Private_IonConstants.isHighSurrogate(c)) {
                    // houston we have a high surrogate (let's hope it has a partner
                    if (++ii >= value.length()) {
                        throw new IllegalArgumentException("invalid string, unpaired high surrogate character");
                    }
                    int c2 = value.charAt(ii);
                    if (!_Private_IonConstants.isLowSurrogate(c2)) {
                        throw new IllegalArgumentException("invalid string, unpaired high surrogate character");
                    }
                    c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                }
                else if (_Private_IonConstants.isLowSurrogate(c)) {
                    // it's a loner low surrogate - that's an error
                    throw new IllegalArgumentException("invalid string, unpaired low surrogate character");
                }
            }

            for (c = makeUTF8IntFromScalar(c); (c & 0xffffff00) != 0; ++len) {
                userstream.write((byte)(c & 0xff));
                c = c >>> 8;
            }
        }
        return len;
    }

    // TODO: move this to IonConstants or IonUTF8
    static final public int makeUTF8IntFromScalar(int c) throws IOException
    {
        // TO DO: check this encoding, it is from:
        //      http://en.wikipedia.org/wiki/UTF-8
        // we probably should use some sort of Java supported
        // library for this.  this class might be of interest:
        //     CharsetDecoder(Charset cs, float averageCharsPerByte, float maxCharsPerByte)
        // in: java.nio.charset.CharsetDecoder

        int value = 0;

        // first the quick, easy and common case - ascii
        if (c < 0x80) {
            value = (0xff & c );
        }
        else if (c < 0x800) {
            // 2 bytes characters from 0x000080 to 0x0007FF
            value  = ( 0xff & (0xC0 | (c >> 6)        ) );
            value |= ( 0xff & (0x80 | (c       & 0x3F)) ) <<  8;
        }
        else if (c < 0x10000) {
            // 3 byte characters from 0x800 to 0xFFFF
            // but only 0x800...0xD7FF and 0xE000...0xFFFF are valid
            if (c > 0xD7FF && c < 0xE000) {
                throwUTF8Exception();
            }
            value  = ( 0xff & (0xE0 |  (c >> 12)       ) );
            value |= ( 0xff & (0x80 | ((c >>  6) & 0x3F)) ) <<  8;
            value |= ( 0xff & (0x80 |  (c        & 0x3F)) ) << 16;

        }
        else if (c <= 0x10FFFF) {
            // 4 byte characters 0x010000 to 0x10FFFF
            // these are are valid
            value  = ( 0xff & (0xF0 |  (c >> 18)) );
            value |= ( 0xff & (0x80 | ((c >> 12) & 0x3F)) ) <<  8;
            value |= ( 0xff & (0x80 | ((c >>  6) & 0x3F)) ) << 16;
            value |= ( 0xff & (0x80 |  (c        & 0x3F)) ) << 24;
        }
        else {
            throwUTF8Exception();
        }
        return value;
    }
    static void throwUTF8Exception() throws IOException
    {
        throw new IOException("Invalid UTF-8 character encountered");
    }


    public static class BufferManager
    {
        BlockedBuffer    _buf;
        IonBinary.Reader _reader;
        IonBinary.Writer _writer;

        public BufferManager() {
            _buf = new BlockedBuffer();
            this.openReader();
            this.openWriter();
        }
        public BufferManager(BlockedBuffer buf) {
            _buf = buf;
            this.openReader();
            this.openWriter();
        }

        /**
         * Creates a new buffer containing the entire content of an
         * {@link InputStream}.
         *
         * @param bytestream a stream interface the byte image to buffer
         *
         * @throws IonException wrapping any {@link IOException}s thrown by the
         * stream.
         */
        public BufferManager(InputStream bytestream)
        {
            this(); // this will create a fresh buffer
                    // as well as a reader and writer

            // now we move the data from the input stream to the buffer
            // more or less as fast as we can.  I am (perhaps foolishly)
            // assuming the "available()" is a useful size.
            try
            {
                _writer.write(bytestream);
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
        }

        /**
         * Creates a new buffer containing the entire content of an
         * {@link InputStream}.
         *
         * @param bytestream a stream interface the byte image to buffer
         *
         * @throws IonException wrapping any {@link IOException}s thrown by the
         * stream.
         */
        public BufferManager(InputStream bytestream, int len)
        {
            this(); // this will create a fresh buffer
                    // as well as a reader and writer

            // now we move the data from the input stream to the buffer
            // more or less as fast as we can.  I am (perhaps foolishly)
            // assuming the "available()" is a useful size.
            try
            {
                _writer.write(bytestream, len);
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }
        }

        @Override
        public BufferManager clone() throws CloneNotSupportedException
        {
            BlockedBuffer buffer_clone = this._buf.clone();
            BufferManager clone = new BufferManager(buffer_clone);
            return clone;
        }

        public IonBinary.Reader openReader() {
            if (_reader == null) {
                _reader = new IonBinary.Reader(_buf);
            }
            return _reader;
        }
        public IonBinary.Writer openWriter() {
            if (_writer == null) {
                _writer = new IonBinary.Writer(_buf);
            }
            return _writer;
        }

        public BlockedBuffer    buffer() { return _buf; }
        public IonBinary.Reader reader() { return _reader; }
        public IonBinary.Writer writer() { return _writer; }

        public IonBinary.Reader reader(int pos) throws IOException
        {
            _reader.setPosition(pos);
            return _reader;
        }
        public IonBinary.Writer writer(int pos) throws IOException
        {
            _writer.setPosition(pos);
            return _writer;
        }
        public static BufferManager makeReadManager(BlockedBuffer buf) {
            BufferManager bufmngr = new BufferManager(buf);
            bufmngr.openReader();
            return bufmngr;
        }
        public static BufferManager makeReadWriteManager(BlockedBuffer buf) {
            BufferManager bufmngr = new BufferManager(buf);
            bufmngr.openReader();
            bufmngr.openWriter();
            return bufmngr;
        }
    }

    /**
     * Variable-length, high-bit-terminating integer, 7 data bits per byte.
     */
    public static int lenVarUInt(long longVal) {
        assert longVal >= 0;
        if (longVal < (1L << (7 * 1))) return 1;  // 7  bits
        if (longVal < (1L << (7 * 2))) return 2;  // 14 bits
        if (longVal < (1L << (7 * 3))) return 3;  // 21 bits
        if (longVal < (1L << (7 * 4))) return 4;  // 28 bits
        if (longVal < (1L << (7 * 5))) return 5;  // 35 bits
        if (longVal < (1L << (7 * 6))) return 6;  // 42 bits
        if (longVal < (1L << (7 * 7))) return 7;  // 49 bits
        if (longVal < (1L << (7 * 8))) return 8;  // 56 bits
        if (longVal < (1L << (7 * 9))) return 9;  // 63 bits
        return 10;
    }

    // TODO maybe add lenVarInt(int) to micro-optimize, or?

    public static int lenVarInt(long longVal) {
        if (longVal == 0) {
            return 0;
        }
        if (longVal <  0) longVal = -longVal;
        if (longVal < (1L << (7 * 1 - 1))) return 1;  // 6  bits
        if (longVal < (1L << (7 * 2 - 1))) return 2;  // 13 bits
        if (longVal < (1L << (7 * 3 - 1))) return 3;  // 20 bits
        if (longVal < (1L << (7 * 4 - 1))) return 4;  // 27 bits
        if (longVal < (1L << (7 * 5 - 1))) return 5;  // 34 bits
        if (longVal < (1L << (7 * 6 - 1))) return 6;  // 41 bits
        if (longVal < (1L << (7 * 7 - 1))) return 7;  // 48 bits
        if (longVal < (1L << (7 * 8 - 1))) return 8;  // 55 bits
        if (longVal < (1L << (7 * 9 - 1))) return 9;  // 62 bits
        return 10;
    }

    /**
     * @return zero if input is zero
     */
    public static int lenUInt(long longVal) {
        if (longVal == 0) {
            return 0;
        }
        if (longVal < 0) {
            throw new BlockedBuffer.BlockedBufferException("fatal signed long where unsigned was promised");
        }
        if (longVal < (1L << (8 * 1))) return 1;  // 8  bits
        if (longVal < (1L << (8 * 2))) return 2;  // 16 bits
        if (longVal < (1L << (8 * 3))) return 3;  // 24 bits
        if (longVal < (1L << (8 * 4))) return 4;  // 32 bits
        if (longVal < (1L << (8 * 5))) return 5;  // 40 bits
        if (longVal < (1L << (8 * 6))) return 6;  // 48 bits
        if (longVal < (1L << (8 * 7))) return 7;  // 56 bits
        return 8;
    }

    public static int lenUInt(BigInteger bigVal)
    {
        if (bigVal.signum() < 0) {
            throw new IllegalArgumentException("lenUInt expects a non-negative a value");
        }
        final int bits = bigVal.bitLength();
        // determine the number of octets needed to represent this bit pattern
        // (div 8)
        int bytes = bits >> 3;
        // if we have a bit more than what can fit in an octet, we need to add
        // an extra octet (mod 8)
        if ((bits & 0x7) != 0)
        {
            bytes++;
        }
        return bytes;
    }

    // TODO maybe add lenInt(int) to micro-optimize, or?

    public static int lenInt(long longVal) {
        if (longVal != 0) {
            return 0;
        }
        if (longVal < 0) longVal = -longVal;
        if (longVal < (1L << (8 * 1 - 1))) return 1;   // 7  bits
        if (longVal < (1L << (8 * 2 - 1))) return 2;   // 15 bits
        if (longVal < (1L << (8 * 3 - 1))) return 3;   // 23 bits
        if (longVal < (1L << (8 * 4 - 1))) return 4;   // 31 bits
        if (longVal < (1L << (8 * 5 - 1))) return 5;   // 39 bits
        if (longVal < (1L << (8 * 6 - 1))) return 6;   // 47 bits
        if (longVal < (1L << (8 * 7 - 1))) return 7;   // 55 bits
        if (longVal == Long.MIN_VALUE) return 9;
        return 8;
    }

    public static int lenInt(BigInteger bi, boolean force_zero_writes)
    {
        int len = bi.abs().bitLength() + 1; // add 1 for the sign bit (which must always be present)
        int bytelen = 0;

        switch (bi.signum()) {
        case 0:
            bytelen = force_zero_writes ? 1 : 0;
            break;
        case 1:
        case -1:
            bytelen = ((len-1) / 8) + 1;
            break;
        }

        return bytelen;
    }

    public static int lenIonInt(long v) {
        if (v < 0) {
            // note that for Long.MIN_VALUE (0x8000000000000000) the negative
            //      is the same, but that's also the bit pattern we need to
            //      write out, but the UInt method won't like it, so we just
            //      return then value that we actually know.
            if (v == Long.MIN_VALUE) return SIZE_OF_LONG;
            return IonBinary.lenUInt(-v);
        }
        else if (v > 0) {
            return IonBinary.lenUInt(v);
        }
        return 0; // CAS UPDATE, was 1
    }

    public static int lenIonInt(BigInteger v)
    {
        if (v.signum() < 0)
        {
            v = v.negate();
        }
        int len = lenUInt(v);
        return len;
    }

    /**
     * The size of length value when short lengths are recorded in the
     * typedesc low-nibble.
     * @param valuelen
     * @return zero if valuelen < 14
     */
    public static int lenLenFieldWithOptionalNibble(int valuelen) {
        if (valuelen < _Private_IonConstants.lnIsVarLen) {
            return 0;
        }
        return lenVarUInt(valuelen);
    }

    public static int lenTypeDescWithAppropriateLenField(int type, int valuelen)
    {
        switch (type) {
        case _Private_IonConstants.tidNull: // null(0)
        case _Private_IonConstants.tidBoolean: // boolean(1)
            return _Private_IonConstants.BB_TOKEN_LEN;

        case _Private_IonConstants.tidPosInt: // 2
        case _Private_IonConstants.tidNegInt: // 3
        case _Private_IonConstants.tidFloat: // float(4)
        case _Private_IonConstants.tidDecimal: // decimal(5)
        case _Private_IonConstants.tidTimestamp: // timestamp(6)
        case _Private_IonConstants.tidSymbol: // symbol(7)
        case _Private_IonConstants.tidString: // string (8)
        case _Private_IonConstants.tidClob: // clob(9)
        case _Private_IonConstants.tidBlob: // blob(10)
        case _Private_IonConstants.tidList:   // 11        -- cas mar 6 2008, moved containers
        case _Private_IonConstants.tidSexp:   // 12        -- up heresince they now use the same
        case _Private_IonConstants.tidStruct: // 13        -- length encodings as scalars
        case _Private_IonConstants.tidTypedecl: // 14
            if (valuelen < _Private_IonConstants.lnIsVarLen) {
                return _Private_IonConstants.BB_TOKEN_LEN;
            }
            return _Private_IonConstants.BB_TOKEN_LEN + lenVarUInt(valuelen);

        case _Private_IonConstants.tidUnused: // unused(15)
        default:
            throw new IonException("invalid type");
        }
    }

    public static int lenIonFloat(double value) {
        if (Double.valueOf(value).equals(DOUBLE_POS_ZERO))
        {
            // pos zero special case
            return 0;
        }

        // always 8-bytes for IEEE-754 64-bit
        return _ib_FLOAT64_LEN;
    }

    /**
     * Would this value have a zero length-nibble?  That is: is it 0d0 ?
     */
    public static boolean isNibbleZero(BigDecimal bd)
    {
        if (Decimal.isNegativeZero(bd)) return false;
        if (bd.signum() != 0) return false;
        int scale = bd.scale();
        return (scale == 0);
    }

    /**
     * @param bd must not be null.
     */
    public static int lenIonDecimal(BigDecimal bd)
    {
        // first check for the special cases of null
        // and 0d0 which are encoded in the nibble
        if (bd == null) return 0;
        if (isNibbleZero(bd)) return 0;

        // otherwise do this the hard way
        BigInteger mantissa = bd.unscaledValue();

        // negative zero mantissa must be written, positive zero does not
        boolean forceMantissa = Decimal.isNegativeZero(bd);
        int mantissaByteCount = lenInt(mantissa, forceMantissa);

        // We really need the length of the exponent (-scale) but in our
        // representation the length is the same regardless of sign.
        int scale = bd.scale();
        int exponentByteCount = lenVarInt(scale);

        // Exponent is always at least one byte.
        if (exponentByteCount == 0) exponentByteCount = 1;

        return exponentByteCount + mantissaByteCount;
    }


    /**
     * this method computes the output length of this timestamp value
     * in the Ion binary format.  It does not include the length of
     * the typedesc byte that preceeds the actual value.  The output
     * length of a null value is 0, as a result this this.
     * @param di may be null
     */
    public static int lenIonTimestamp(Timestamp di)
    {
        if (di == null) return 0;

        int len = 0;
        switch (di.getPrecision()) {
        case FRACTION:
        case SECOND:
        {
            BigDecimal fraction = di.getFractionalSecond();
            if (fraction != null)
            {
                assert fraction.signum() >=0 && ! fraction.equals(BigDecimal.ZERO)
                    : "Bad timestamp fraction: " + fraction;

                // Since the fraction is not 0d0, at least one subfield of the
                // exponent and mantissa is non-zero, so this will always write at
                // least one byte.
                int fracLen = IonBinary.lenIonDecimal(fraction);
                assert fracLen > 0;
                len += fracLen;
            }

            len++; // len of seconds < 60
        }
        case MINUTE:
            len += 2; // len of hour and minutes (both < 127)
        case DAY:
            len += 1; // len of month and day (both < 127)
        case MONTH:
            len += 1; // len of month and day (both < 127)
        case YEAR:
            len += IonBinary.lenVarUInt(di.getZYear());
        }
        Integer offset = di.getLocalOffset();
        if (offset == null) {
            len++; // room for the -0 (i.e. offset is "no specified offset")
        }
        else if (offset == 0) {
            len++;
        }
        else {
            len += IonBinary.lenVarInt(offset.longValue());
        }
        return len;
    }

    /**
     * @param v may be null.
     * @throws IllegalArgumentException if the text contains bad UTF-16 data.
     */
    public static int lenIonString(String v)
    {
        if (v == null) return 0;
        int len = 0;

        for (int ii=0; ii<v.length(); ii++) {
            int c = v.charAt(ii);

            // handle the cheap characters quickly
            if (c < 128) {
                len++;
                continue;
            }
            // multi-byte utf8
            if (c >= 0xD800 && c <= 0xDFFF) {
                // look for surrogate pairs and merge them (and throw on bad data)
                if (_Private_IonConstants.isHighSurrogate(c)) {
                    ii++;
                    if (ii >= v.length()) {
                        String message =
                            "Text ends with unmatched UTF-16 surrogate " +
                            IonTextUtils.printCodePointAsString(c);
                        throw new IllegalArgumentException(message);
                    }
                    int c2 = v.charAt(ii);
                    if (!_Private_IonConstants.isLowSurrogate(c2)) {
                        String message =
                            "Text contains unmatched UTF-16 high surrogate " +
                            IonTextUtils.printCodePointAsString(c) +
                            " at index " + (ii-1);
                        throw new IllegalArgumentException(message);
                    }
                    c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                }
                else if (_Private_IonConstants.isLowSurrogate(c)) {
                    String message =
                        "Text contains unmatched UTF-16 low surrogate " +
                        IonTextUtils.printCodePointAsString(c) +
                        " at index " + ii;
                    throw new IllegalArgumentException(message);
                }
            }
            // no need to check the 0x10FFFF overflow as it is checked in lenUnicodeScalarAsUTF8

            // and now figure out how long this "complicated" (non-ascii) character is
            if (c < 0x80) {
                ++len;
            }
            else if (c < 0x800) {
                len += 2;
            }
            else if (c < 0x10000) {
                len += 3;
            }
            else if (c <= 0x10FFFF) { // just about as cheap as & == 0 and checks for some out of range values
                len += 4;
            } else {
                // TODO how is this possible?
                throw new IllegalArgumentException("invalid string, illegal Unicode scalar (character) encountered");
            }
        }

        return len;
    }

    public static int lenAnnotationListWithLen(String[] annotations,
                                               SymbolTable symbolTable)
    {
        int annotationLen = 0;

        if (annotations != null) {
            // add up the length of the encoded symbols
            for (int ii=0; ii<annotations.length; ii++) {
                int symid = symbolTable.findSymbol(annotations[ii]);
                assert symid > 0; // TODO amazon-ion/ion-java/issues/12
                annotationLen += IonBinary.lenVarUInt(symid);
            }

            // now the len of the list
            annotationLen += IonBinary.lenVarUInt(annotationLen);
        }
        return annotationLen;
    }

    public static int lenAnnotationListWithLen(ArrayList<Integer> annotations)
    {
        int annotationLen = 0;

        // add up the length of the encoded symbols
        for (Integer ii : annotations) {
            int symid = ii.intValue();
            annotationLen += IonBinary.lenVarUInt(symid);
        }

        // now the len of the list
        annotationLen += IonBinary.lenVarUInt(annotationLen);

        return annotationLen;
    }

    public static int lenIonNullWithTypeDesc() {
        return _ib_TOKEN_LEN;
    }
    public static int lenIonBooleanWithTypeDesc(Boolean v) {
        return _ib_TOKEN_LEN;
    }
    public static int lenIonIntWithTypeDesc(Long v) {
        int len = 0;
        if (v != null) {
            long vl = v.longValue();
            int vlen = lenIonInt(vl);
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }
    public static int lenIonFloatWithTypeDesc(Double v) {
        int len = 0;
        if (v != null) {
            int vlen = lenIonFloat(v);
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }
    public static int lenIonDecimalWithTypeDesc(BigDecimal v) {
        int len = 0;
        if (v != null) {
            int vlen = lenIonDecimal(v);
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }
    public static int lenIonTimestampWithTypeDesc(Timestamp di) {
        int len = 0;
        if (di != null) {
            int vlen = IonBinary.lenIonTimestamp(di);
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }
    public static int lenIonStringWithTypeDesc(String v) {
        int len = 0;
        if (v != null) {
            int vlen = lenIonString(v);
            if (vlen < 0) return -1;
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }
    public static int lenIonClobWithTypeDesc(String v) {
        return lenIonStringWithTypeDesc(v);
    }
    public static int lenIonBlobWithTypeDesc(byte[] v) {
        int len = 0;
        if (v != null) {
            int vlen = v.length;
            len += vlen;
            len += lenLenFieldWithOptionalNibble(vlen);
        }
        return len + _ib_TOKEN_LEN;
    }


    /** Utility method to convert an unsigned magnitude stored as a long to a {@link BigInteger}. */
    public static BigInteger unsignedLongToBigInteger(int signum, long val)
    {
        byte[] magnitude = {
            (byte) ((val >> 56) & 0xFF),
            (byte) ((val >> 48) & 0xFF),
            (byte) ((val >> 40) & 0xFF),
            (byte) ((val >> 32) & 0xFF),
            (byte) ((val >> 24) & 0xFF),
            (byte) ((val >> 16) & 0xFF),
            (byte) ((val >>  8) & 0xFF),
            (byte) (val & 0xFF),
        };
        return new BigInteger(signum, magnitude);
    }

    /**
     * Uses {@link InputStream#read(byte[], int, int)} until the entire length is read.
     * This method will block until the request is satisfied.
     *
     * @param in        The stream to read from.
     * @param buf       The buffer to read to.
     * @param offset    The offset of the buffer to read from.
     * @param len       The length of the data to read.
     */
    public static void readAll(InputStream in, byte[] buf, int offset, int len) throws IOException
    {
        int rem = len;
        while (rem > 0)
        {
            int amount = in.read(buf, offset, rem);
            if (amount <= 0)
            {
                // try to throw a useful exception
                if (in instanceof Reader)
                {
                    ((Reader) in).throwUnexpectedEOFException();
                }
                // defer to a plain exception
                throw new IonException("Unexpected EOF");
            }
            rem -= amount;
            offset += amount;
        }
    }

    public static final class Reader
        extends BlockedBuffer.BlockedByteInputStream
    {
        /**
         * @param bb blocked buffer to read from
         */
        public Reader(BlockedBuffer bb)
        {
            super(bb);
        }
        /**
         * @param bb blocked buffer to read from
         * @param pos initial offset to read
         */
        public Reader(BlockedBuffer bb, int pos)
        {
            super(bb, pos);
        }

        /**
         * return the underlying bytes as a single buffer
         *
         * @return bytes[]
         * @throws IOException
         * @throws UnexpectedEofException if end of file is hit.
         * @throws IOException if there's other problems reading input.
         */
        public byte[] getBytes() throws IOException {
            if (this._buf == null) return null;
            this.sync();
            this.setPosition(0);
            int len = _buf.size();
            byte[] buf = new byte[len];
            if (readFully(this, buf) != len) {
                throw new UnexpectedEofException();
            }
            return buf;
        }


        /**
         * Read exactly one byte of input.
         *
         * @return 0x00 through 0xFF as a positive int.
         * @throws UnexpectedEofException if end of file is hit.
         * @throws IOException if there's other problems reading input.
         */
        public int readToken() throws UnexpectedEofException, IOException
        {
            int c = read();
            if (c < 0) throwUnexpectedEOFException();
            return c;
        }

        public int readActualTypeDesc() throws IOException
        {
            int c = read();
            if (c < 0) throwUnexpectedEOFException();
            int typeid = _Private_IonConstants.getTypeCode(c);
            if (typeid == _Private_IonConstants.tidTypedecl) {
                int lownibble = _Private_IonConstants.getLowNibble(c);
                if (lownibble == 0) {
                    // 0xE0 is the first byte of the IonVersionMarker
                    // so we'll return it as is, the caller has to
                    // verify the remaining bytes and handle them
                    // appropriately - so here we do nothing
                }
                else {
                    this.readLength(typeid, lownibble);
                    int alen = this.readVarIntAsInt();
                    // TODO add skip(int) method instead of this loop.
                    while (alen > 0) {
                        if (this.read() < 0) throwUnexpectedEOFException();
                        alen--;
                    }
                    c = read();
                    if (c < 0) throwUnexpectedEOFException();
                }
            }
            return c;
        }

        public int[] readAnnotations() throws IOException
        {
            int[] annotations = null;

            int annotationLen = this.readVarUIntAsInt();
            int annotationPos = this.position(); // pos at the first ann sid
            int annotationEnd = annotationPos + annotationLen;
            int annotationCount = 0;

            // first we read through and count
            while(this.position() < annotationEnd) {
                // read the annotation symbol id itself
                // and for this first pass we just throw that
                // value away, since we're just counting
                this.readVarUIntAsInt();
                annotationCount++;
            }
            if (annotationCount > 0) {
                // then, if there are any there, we
                // allocate the array, and re-read the sids
                // look them up and fill in the array
                annotations = new int[annotationCount];
                int annotationIdx = 0;
                this.setPosition(annotationPos);
                while(this.position() < annotationEnd) {
                    // read the annotation symbol id itself
                    int sid = this.readVarUIntAsInt();
                    annotations[annotationIdx++] = sid;
                }
            }

            return annotations;
        }


        public int readLength(int td, int ln) throws IOException
        {
            // TODO check for invalid lownibbles
            switch (td) {
            case _Private_IonConstants.tidNull: // null(0)
            case _Private_IonConstants.tidBoolean: // boolean(1)
                return 0;
            case _Private_IonConstants.tidPosInt: // 2
            case _Private_IonConstants.tidNegInt: // 3
            case _Private_IonConstants.tidFloat: // float(4)
            case _Private_IonConstants.tidDecimal: // decimal(5)
            case _Private_IonConstants.tidTimestamp: // timestamp(6)
            case _Private_IonConstants.tidSymbol: // symbol(7)
            case _Private_IonConstants.tidString: // string (8)
            case _Private_IonConstants.tidClob: // clob(9)
            case _Private_IonConstants.tidBlob: // blob(10)
            case _Private_IonConstants.tidList:     // 11
            case _Private_IonConstants.tidSexp:     // 12
            case _Private_IonConstants.tidTypedecl: // 14
                switch (ln) {
                case 0:
                case _Private_IonConstants.lnIsNullAtom:
                    return 0;
                case _Private_IonConstants.lnIsVarLen:
                    return readVarUIntAsInt();
                default:
                    return ln;
                }
            case _Private_IonConstants.tidNopPad:   // 99
                switch (ln) {
                    case _Private_IonConstants.lnIsVarLen:
                        return readVarUIntAsInt();
                    default:
                        return ln;
                }
            case _Private_IonConstants.tidStruct: // 13
                switch (ln) {
                case _Private_IonConstants.lnIsEmptyContainer:
                case _Private_IonConstants.lnIsNullStruct:
                    return 0;
                case _Private_IonConstants.lnIsOrderedStruct:
                case _Private_IonConstants.lnIsVarLen:
                    return readVarUIntAsInt();
                default:
                    return ln;
                }
            case _Private_IonConstants.tidUnused: // unused(15)
            default:
                // TODO use InvalidBinaryDataException
                throw new BlockedBuffer.BlockedBufferException("invalid type id encountered: " + td);
            }
        }

        /*
        public long readFixedIntLongValue(int len) throws IOException {
            long retvalue = 0;
            int b;

            switch (len) {
            case 8:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (7*8);
            case 7:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (6*8);
            case 6:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (5*8);
            case 5:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (4*8);
            case 4:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (3*8);
            case 3:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (2*8);
            case 2:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (1*8);
            case 1:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (0*8);
            }
            return retvalue;
        }
        public int readFixedIntIntValue(int len) throws IOException {
            int retvalue = 0;
            int b;

            switch (len) {
            case 4:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (3*8);
            case 3:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (2*8);
            case 2:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (1*8);
            case 1:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue |= b << (0*8);
            }
            return retvalue;
        }
        */
        public long readIntAsLong(int len) throws IOException {
            long    retvalue = 0;
            boolean is_negative = false;
            int     b;

            if (len > 0) {
                // read the first byte
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (b & 0x7F);
                is_negative = ((b & 0x80) != 0);

                switch (len - 1) {  // we read 1 already
                case 8: // even after reading the 1st byte we may have 8 remaining
                        // bytes of value when the value is Long.MIN_VALUE since it
                        // has all 8 bytes for data and the ninth for the sign bit
                        // all by itself (which we read above)
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
                default:
                }
                if (is_negative) {
                    // this is correct even when retvalue == Long.MIN_VALUE
                    retvalue = -retvalue;
                }
            }
            return retvalue;
        }
        /** @throws IOException
         * @deprecated */
        @Deprecated
        public int readIntAsInt(int len) throws IOException {
            int retvalue = 0;
            boolean is_negative = false;
            int b;

            if (len > 0) {
                // read the first byte
                b = read();
                if (b < 0) throwUnexpectedEOFException();
                retvalue = (b & 0x7F);
                is_negative = ((b & 0x80) != 0);

                switch (len - 1) {  // we read 1 already
                case 7:
                case 6:
                case 5:
                case 4:
                    throw new IonException("overflow attempt to read long value into an int");
                case 3:
                    if ((b = read()) < 0) throwUnexpectedEOFException();
                    retvalue = (retvalue << 8) | b;
                case 2:
                    if ((b = read()) < 0) throwUnexpectedEOFException();
                    retvalue = (retvalue << 8) | b;

                    if ((b = read()) < 0) throwUnexpectedEOFException();
                    retvalue = (retvalue << 8) | b;
                }
                if (is_negative) {
                    // this is correct even when retvalue == Integer.MIN_VALUE
                    retvalue = -retvalue;
                }
            }
            return retvalue;
        }

        /**
         * Reads the specified magnitude as a {@link BigInteger}.
         *
         * @param len           The length of the UInt octets to read.
         * @param signum        The sign as per {@link BigInteger#BigInteger(int, byte[])}.
         * @return              The signed {@link BigInteger}.
         *
         * @throws IOException  Thrown if there are an I/O errors on the underlying stream.
         */
        public BigInteger readUIntAsBigInteger(int len, int signum) throws IOException {
            byte[] magnitude = new byte[len];
            for (int i = 0; i < len; i++) {
                int octet = read();
                if (octet < 0) {
                    throwUnexpectedEOFException();
                }
                magnitude[i] = (byte) octet;
            }

            // both BigInteger and ion store this magnitude as big-endian
            return new BigInteger(signum, magnitude);
        }

        public long readUIntAsLong(int len) throws IOException {
            long    retvalue = 0;
            int b;

            switch (len) {
            default:
                throw new IonException("overflow attempt to read long value into an int");
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
            }
            return retvalue;
        }
        public int readUIntAsInt(int len) throws IOException {
            int retvalue = 0;
            int b;

            switch (len) {
            default:
                throw new IonException("overflow attempt to read long value into an int");
            case 4:
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = b;
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
            }
            return retvalue;
        }
        public long readVarUIntAsLong() throws IOException {
            long retvalue = 0;
            int  b;

            for (;;) {
                if ((b = read()) < 0) throwUnexpectedEOFException();
                if ((retvalue & 0xFE00000000000000L) != 0) {
                    // if any of the top 7 bits are set at this point there's
                    // a problem, because we'll be shifting the into oblivian
                    // just below, so ...
                    throw new IonException("overflow attempt to read long value into a long");
                }
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;
            }
            return retvalue;
        }
        public int readVarUIntAsInt() throws IOException {
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
                throw new IonException("var int overflow at: "+this.position());
            }
            return retvalue;
        }
        public long readVarIntAsLong() throws IOException
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
                    if ((retvalue & 0xFE00000000000000L) != 0) {
                        // if any of the top 7 bits are set at this point there's
                        // a problem, because we'll be shifting the into oblivian
                        // just below, so ...
                        throw new IonException("overflow attempt to read long value into a long");
                    }
                    retvalue = (retvalue << 7) | (b & 0x7F);
                    if ((b & 0x80) != 0) break done;
                }
            }
            if (is_negative) {
                // this is correct even when retvalue == Long.MIN_VALUE
                retvalue = -retvalue;
            }
            return retvalue;
        }
        public int readVarIntAsInt() throws IOException
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
                throw new IonException("var int overflow at: "+this.position());
            }
            if (is_negative) {
                // this is correct even when retvalue == Integer.MIN_VALUE
                retvalue = -retvalue;
            }
            return retvalue;
        }

        /**
         * Reads an integer value, returning null to mean -0.
         * @throws IOException
         */
        public Integer readVarIntWithNegativeZero() throws IOException
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
                throw new IonException("var int overflow at: "+this.position());
            }

            Integer retInteger = null;
            if (is_negative) {
                if (retvalue != 0) {
                    // this is correct even when retvalue == Long.MIN_VALUE
                    retvalue = -retvalue;
                    retInteger = Integer.valueOf(retvalue);
                }
            }
            else {
                retInteger = Integer.valueOf(retvalue);
            }
            return retInteger;
        }

        public double readFloatValue(int len) throws IOException
        {
            if (len == 0)
            {
                // special case, return pos zero
                return 0.0d;
            }

            if (len != 8)
            {
                throw new IonException("Length of float read must be 0 or 8");
            }

            long dBits = readUIntAsLong(len);
            return Double.longBitsToDouble(dBits);
        }


        /**
         * Near clone of {@link SimpleByteBuffer.SimpleByteReader#readDecimal(int)}
         * and {@link IonReaderBinaryRawX#readDecimal(int)}
         * so keep them in sync!
         */
        public Decimal readDecimalValue(int len) throws IOException
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
                int         exponent = this.readVarIntAsInt();
                int         bitlen = len - (this.position() - startpos);

                BigInteger value;
                int        signum;
                if (bitlen > 0)
                {
                    byte[] bits = new byte[bitlen];
                    readAll(this, bits, 0, bitlen);

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

        /**
         * @see IonReaderBinaryRawX#readTimestamp
         */
        public Timestamp readTimestampValue(int len) throws IOException
        {
            if (len < 1) {
                // nothing to do here - and the timestamp will be NULL
                return null;
            }

            int        year = 0, month = 0, day = 0, hour = 0, minute = 0, second = 0;
            Decimal    frac = null;
            int        end = this.position() + len;

            // first up is the offset, which requires a special int reader
            // to return the -0 as a null Integer
            Integer offset = this.readVarIntWithNegativeZero();

            // now we'll read the struct values from the input stream

            // year is from 0001 to 9999
            // or 0x1 to 0x270F or 14 bits - 1 or 2 bytes
            year  = readVarUIntAsInt();
            Precision p = Precision.YEAR; // our lowest significant option

            if (position() < end) {
                month = readVarUIntAsInt();
                p = Precision.MONTH; // our lowest significant option

                if (position() < end) {
                    day   = readVarUIntAsInt();
                    p = Precision.DAY; // our lowest significant option

                    // now we look for hours and minutes
                    if (position() < end) {
                        hour   = readVarUIntAsInt();
                        minute = readVarUIntAsInt();
                        p = Precision.MINUTE;

                        if (position() < end) {
                            second = readVarUIntAsInt();
                            p = Precision.SECOND;

                            int remaining = end - position();
                            if (remaining > 0) {
                                // now we read in our actual "milliseconds since the epoch"
                                frac = this.readDecimalValue(remaining);
                            }
                        }
                    }
                }
            }

            // now we let timestamp put it all together
            try {
                Timestamp val =
                    Timestamp.createFromUtcFields(p, year, month, day,
                                                  hour, minute, second,
                                                  frac, offset);
                return val;
            }
            catch (IllegalArgumentException e) {
                throw new IonException(e.getMessage() + " at: " + position());
            }
        }

        public String readString(int len) throws IOException
        {
            char[] cb = new char[len]; // since we know the length in bytes (which must be
                                    // greater than or equal to chars) there's no need
                                    // for a stringbuffer (even surrgated char's are ok)
            int ii=0;
            int c;
            int endPosition = this.position() + len;

            while (this.position() < endPosition) {
                c = readUnicodeScalar();
                if (c < 0) throwUnexpectedEOFException();
                //sb.append((char)c);
                if (c < 0x10000) {
                    cb[ii++] = (char)c;
                }
                else {
                    cb[ii++] = (char)_Private_IonConstants.makeHighSurrogate(c);
                    cb[ii++] = (char)_Private_IonConstants.makeLowSurrogate(c);
                }
            }

            if (this.position() < endPosition) throwUnexpectedEOFException();

            return new String(cb, 0, ii); // sb.toString();
        }
        public int readUnicodeScalar() throws IOException {
            int c = -1, b;

            b = read();
            if (b < 0) return -1;

            // ascii is all good
            if ((b & 0x80) == 0) {
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
                    throw new IonException("illegal surrgate value encountered in input utf-8 stream");
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
                    throw new IonException("illegal surrgate value encountered in input utf-8 stream");
                }
            }
            else {
                throwUTF8Exception();
            }
            return c;
        }
        void throwUTF8Exception()
        {
            throw new IonException("Invalid UTF-8 character encounter in a string at pos " + this.position());
        }
        void throwUnexpectedEOFException() {
            throw new BlockedBuffer.BlockedBufferException("unexpected EOF in value at offset " + this.position());
        }

        public String readString() throws IOException {
            int td = read();
            if (_Private_IonConstants.getTypeCode(td) != _Private_IonConstants.tidString) {
                throw new IonException("readString helper only works for string(7) not "+((td >> 4 & 0xf)));
            }
            int len = (td & 0xf);
            if (len == _Private_IonConstants.lnIsNullAtom) {
                return null;
            }
            else if (len == _Private_IonConstants.lnIsVarLen) {
                len = this.readVarUIntAsInt();
            }
            return readString(len);
        }

        /**
         * Skips through a NOP pad if there is one. Must be called at the beginning of type description
         *
         * It's no-op if there is no Nop pad to skip through at the current position
         *
         * @return true if it skipped through a nop pad
         */
        boolean skipThroughNopPad() throws IOException
        {
            int originalPosition = this._pos;

            // check to see if there is a type declaration as it's not valid to annotate nop pads
            int c = this.read();
            boolean hasTypedecl = _Private_IonConstants.getTypeCode(c) == _Private_IonConstants.tidTypedecl;
            this.setPosition(originalPosition);

            int typeDesc = this.readActualTypeDesc();
            int tid = _Private_IonConstants.getTypeCode(typeDesc);
            int len = _Private_IonConstants.getLowNibble(typeDesc);

            if(tid == _Private_IonConstants.tidNull && len != _Private_IonConstants.lnIsNull){
                if(hasTypedecl) {
                    throw new IonException("NOP padding is not allowed within annotation wrappers.");
                }

                int toSkip = this.readLength(_Private_IonConstants.tidNopPad, len);
                long skipped = this.skip(toSkip);
                if(toSkip > 0 && toSkip != skipped) {
                    throw new IonException("Nop pad too short declared length: " + toSkip + " pad actual size: " + skipped);
                }

                return true;
            }

            // resets the reader if it wasn't a NOP pad
            this.setPosition(originalPosition);

            return false;
        }
    }

    public static final class Writer
        extends BlockedBuffer.BlockedByteOutputStream
    {
        /**
         * creates writable stream (OutputStream) that writes
         * to a fresh blocked buffer.  The stream is initially
         * position at offset 0.
         */
        public Writer() { super(); }
        /**
         * creates writable stream (OutputStream) that writes
         * to the supplied byte buffer.  The stream is initially
         * position at offset 0.
         * @param bb blocked buffer to write to
         */
        public Writer(BlockedBuffer bb) { super(bb); }
        /**
         * creates writable stream (OutputStream) that can write
         * to the supplied byte buffer.  The stream is initially
         * position at offset off.
         * @param bb blocked buffer to write to
         * @param off initial offset to write to
         */
        public Writer(BlockedBuffer bb, int off) { super(bb, off); }

        Stack<PositionMarker> _pos_stack;
        Stack<Integer>        _pending_high_surrogate_stack;
        int                   _pending_high_surrogate;
        public void pushPosition(Object o)
        {
            PositionMarker pm = new PositionMarker(this.position(), o);
            if (_pos_stack == null) {
                _pos_stack = new Stack<PositionMarker>();
                _pending_high_surrogate_stack = new Stack<Integer>();
            }
            _pos_stack.push(pm);
            _pending_high_surrogate_stack.push(_pending_high_surrogate);
            _pending_high_surrogate = 0;
        }
        public PositionMarker popPosition()
        {
            if (_pending_high_surrogate != 0) {
                throw new IonException("unmatched high surrogate encountered in input, illegal utf-16 character sequence");
            }
            PositionMarker pm = _pos_stack.pop();
            _pending_high_surrogate = _pending_high_surrogate_stack.pop();
            return pm;
        }

        /*****************************************************************************
        *
        * These routines work together to write very long values from an input
        * reader where we don't know how long the value is going to be in advance.
        *
        * Basically it tries to write a short value (len <= BB_SHORT_LEN_MAX) and if
        * that fails it moves the data around in the buffers and moves buffers worth
        * of data at a time.
        *
        */
        static class lhNode
        {
           int     _hn;
           int     _lownibble;
           boolean _length_follows;

           lhNode(int hn
                 ,int lownibble
                 ,boolean length_follows
           ) {
               _hn = hn;
               _lownibble = lownibble;
               _length_follows = length_follows;
           }
        }

        public void startLongWrite(int hn) throws IOException
        {
            if (debugValidation) _validate();

            pushLongHeader(hn, 0, false);

            this.writeCommonHeader(hn, 0);

            if (debugValidation) _validate();
        }

        public void pushLongHeader(int  hn
                                  ,int lownibble
                                  ,boolean length_follows
        ) {
           lhNode n = new lhNode(hn, lownibble, length_follows);
           pushPosition(n);
        }


        /**
         * Update the TD/LEN header of a value.
         *
         * @param hn high nibble value (or type id)
         * @param lownibble is the low nibble value.
         * If -1, then the low nibble is pulled from the header node pushed
         * previously.  If the header node had lengthFollows==false then this
         * is ignored and the LN is computed from what was actually written.
         */
        public void patchLongHeader(int hn, int lownibble) throws IOException
        {
           int currpos = this.position();

           if (debugValidation) _validate();

           // get the header description we pushed on the stack a while ago
           // pop also checks to make sure we don't have a dangling high
           // surrogate pending
           PositionMarker pm = this.popPosition();
           lhNode n = (lhNode)pm.getUserData();
           int totallen = currpos - pm.getPosition();

           // fix up the low nibble if we need to
           if (lownibble == -1) {
               lownibble = n._lownibble;
           }

           // calculate the length just the value itself
           // we don't count the type descriptor in the value len
           int writtenValueLen = totallen - _Private_IonConstants.BB_TOKEN_LEN;

           // now we can figure out how long the value is going to be

           // This is the length of the length (it does NOT
           // count the typedesc byte however)
           int len_o_len = IonBinary.lenVarUInt(writtenValueLen);

           // TODO cleanup this logic.  lengthFollows == is struct
           if (n._length_follows) {
               assert hn == _Private_IonConstants.tidStruct;

               if (lownibble == _Private_IonConstants.lnIsOrderedStruct)
               {
                   // leave len_o_len alone
               }
               else
               {
                   if (writtenValueLen < _Private_IonConstants.lnIsVarLen) {
                       lownibble = writtenValueLen;
                       len_o_len = 0;
                   }
                   else {
                       lownibble = _Private_IonConstants.lnIsVarLen;
                   }
                   assert lownibble != _Private_IonConstants.lnIsOrderedStruct;
               }
           }
           else {
               if (writtenValueLen < _Private_IonConstants.lnIsVarLen) {
                   lownibble = writtenValueLen;
                   len_o_len = 0;
               }
               else {
                   lownibble = _Private_IonConstants.lnIsVarLen;
               }
           }

           // first we go back to the beginning
           this.setPosition(pm.getPosition());

           // figure out if we need to move the trailing data to make
           // room for the variable length length
           int needed = len_o_len;
           if (needed > 0) {
               // insert does the heavy lifting of making room for us
               // at the current position for "needed" additional bytes
               insert(needed);
           }

           // so, we have room (or already had enough) now we can write
           // replacement type descriptor and the length and the reset the pos
           this.writeByte(_Private_IonConstants.makeTypeDescriptor(hn, lownibble));
           if (len_o_len > 0) {
               this.writeVarUIntValue(writtenValueLen, true);
           }

           if (needed < 0) {
               // in the unlikely event we wrote more than we needed, now
               // is the time to remove it.  (which does happen from time to time)
               this.remove(-needed);
           }

           // return the cursor to it's correct location,
           // taking into account the added length data
           this.setPosition(currpos + needed);

           if (debugValidation) _validate();

        }

        // TODO - this may have problems with unicode utf16/utf8 conversions
        // note that this reads characters that have already had the escape
        // sequence processed (so we don't want to do that a second time)
        // the chars here were filled by the IonTokenReader which already
        // de-escaped the escaped sequences from the input, so all chars are "real"
        public void appendToLongValue(CharSequence chars, boolean onlyByteSizedCharacters) throws IOException
        {
            if (debugValidation) _validate();

            int len = chars.length(); // TODO is this ever >0 for clob?

            for (int ii = 0; ii < len; ii++)
            {
                int c = chars.charAt(ii);
                if (onlyByteSizedCharacters) {
                    if (c > 255) {
                        throw new IonException("escaped character value too large in clob (0 to 255 only)");
                    }
                    write((byte)(0xff & c));
                }
                else {
                    if (_pending_high_surrogate != 0) {
                        if ((c & _Private_IonConstants.surrogate_mask) != _Private_IonConstants.low_surrogate_value) {
                            throw new IonException("unmatched high surrogate character encountered, invalid utf-16");
                        }
                        c = _Private_IonConstants.makeUnicodeScalar(_pending_high_surrogate, c);
                        _pending_high_surrogate = 0;
                    }
                    else if ((c & _Private_IonConstants.surrogate_mask) == _Private_IonConstants.high_surrogate_value) {
                        ii++;
                        if (ii >= len) {
                            // a trailing high surrogate, we just remember it for later
                            // and (hopefully, and usually, the low surrogate will be
                            // appended shortly
                            _pending_high_surrogate = c;
                            break;
                        }
                        int c2 = chars.charAt(ii);
                        if ((c2 & _Private_IonConstants.surrogate_mask) != _Private_IonConstants.low_surrogate_value) {
                            throw new IonException("unmatched high surrogate character encountered, invalid utf-16");
                    }
                        c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                    }
                    else if ((c & _Private_IonConstants.surrogate_mask) == _Private_IonConstants.low_surrogate_value) {
                        throw new IonException("unmatched low surrogate character encountered, invalid utf-16");
                    }
                    writeUnicodeScalarAsUTF8(c);
                }
            }

            if (debugValidation) _validate();
        }

        // helper for appendToLongValue below - this never cares about surrogates
        // as it only consumes terminators which are ascii
        final boolean isLongTerminator(int terminator, PushbackReader r) throws IOException {
            int c;

            // look for terminator 2 - if it's not there put back what we saw
            c = r.read();
            if (c != terminator) {
                r.unread(c);
                return false;
            }

            // look for terminator 3 - otherwise put back what we saw and the
            // preceeding terminator we found above
            c = r.read();
            if (c != terminator) {
                r.unread(c);
                r.unread(terminator);
                return false;
            }
            // we found 3 in a row - now that's a long terminator
            return true;
        }

        /**
        * Reads the remainder of a quoted string/symbol into this buffer.
        * The closing quotes are consumed from the reader.
        * <p>
        * XXX  WARNING  XXX
        * Almost identical logic is found in
        * {@link IonTokenReader#finishScanString(boolean)}
        *
        * @param terminator the closing quote character.
        * @param longstring
        * @param r
        * @throws IOException
        */
        public void appendToLongValue(int terminator
                                     ,boolean longstring
                                     ,boolean onlyByteSizedCharacters
                                     ,boolean decodeEscapeSequences
                                     ,PushbackReader r
                                     )
           throws IOException, UnexpectedEofException
        {
            int c;

            if (debugValidation) {
                if (terminator == -1 && longstring) {
                    throw new IllegalStateException("longstrings have to have a terminator, no eof termination");
                }
                _validate();
            }

            assert(terminator != '\\');
            for (;;) {
                c = r.read();  // we put off the surrogate logic as long as possible (so not here)

                if (c == terminator) {
                    if (!longstring || isLongTerminator(terminator, r)) {
                        // if it's not a long string one quote is enough otherwise look ahead
                        break;
                    }
                }
                else if (c == -1) {
                    throw new UnexpectedEofException();
                }
                else if (c == '\n' || c == '\r') {
                    // here we'll handle embedded new line detection and escaped characters
                    if ((terminator != -1) && !longstring) {
                        throw new IonException("unexpected line terminator encountered in quoted string");
                    }
                        }
                else if (decodeEscapeSequences && c == '\\') {
                    // if this is an escape sequence we need to process it now
                    // since we allow a surrogate to be encoded using \ u (or \ U)
                    // encoding
                    c = IonTokenReader.readEscapedCharacter(r, onlyByteSizedCharacters);
                    if (c == IonTokenReader.EMPTY_ESCAPE_SEQUENCE) {
                        continue;
                    }
                }

                if (onlyByteSizedCharacters) {
                    assert(_pending_high_surrogate == 0); // if it's byte sized only, then we shouldn't have a dangling surrogate
                    if ((c & (~0xff)) != 0) {
                        throw new IonException("escaped character value too large in clob (0 to 255 only)");
                    }
                    write((byte)(0xff & c));
                }
                else {
                    // for larger characters we have to glue together surrogates, regardless
                    // of how they were encoded.  If we have a high surrogate and go to peek
                    // for the low surrogate and hit the end of a segment of a long string
                    // (triple quoted multi-line string) we leave a dangling high surrogate
                    // that will get picked up on the next call into this routine when the
                    // next segment of the long string is processed
                    if (_pending_high_surrogate != 0) {
                        if ((c & _Private_IonConstants.surrogate_mask) != _Private_IonConstants.low_surrogate_value) {
                            String message =
                                "Text contains unmatched UTF-16 high surrogate " +
                                IonTextUtils.printCodePointAsString(_pending_high_surrogate);
                            throw new IonException(message);
                        }
                        c = _Private_IonConstants.makeUnicodeScalar(_pending_high_surrogate, c);
                        _pending_high_surrogate = 0;
                    }
                    else if ((c & _Private_IonConstants.surrogate_mask) == _Private_IonConstants.high_surrogate_value) {
                        int c2 = r.read();
                        if (c2 == terminator) {
                            if (longstring && isLongTerminator(terminator, r)) {
                                // if it's a long string termination we'll hang onto the current c as the pending surrogate
                                _pending_high_surrogate = c;
                                c = terminator;
                                break;
                            }
                            // otherwise this is an error
                            String message =
                                "Text contains unmatched UTF-16 high surrogate " +
                                IonTextUtils.printCodePointAsString(c);
                            throw new IonException(message);
                        }
                        else if (c2 == -1) {
                            // eof is also an error - really two errors
                            throw new UnexpectedEofException();
                        }
                        //here we convert escape sequences into characters and continue until
                        //we encounter a non-newline escape (typically immediately)
                        while (decodeEscapeSequences && c2 == '\\') {
                            c2 = IonTokenReader.readEscapedCharacter(r, onlyByteSizedCharacters);
                            if (c2 != IonTokenReader.EMPTY_ESCAPE_SEQUENCE) break;
                            c2 = r.read();
                            if (c2 == terminator) {
                                if (longstring && isLongTerminator(terminator, r)) {
                                    // if it's a long string termination we'll hang onto the current c as the pending surrogate
                                    _pending_high_surrogate = c;
                                    c = c2; // we'll be checking this below
                                    break;
                                }
                                // otherwise this is an error
                                String message =
                                    "Text contains unmatched UTF-16 high surrogate " +
                                    IonTextUtils.printCodePointAsString(c);
                                throw new IonException(message);
                            }
                            else if (c2 == -1) {
                                // eof is also an error - really two errors
                                throw new UnexpectedEofException();
                            }
                        }
                        // check to see how we broke our of the while loop above, we may be "done"
                        if (_pending_high_surrogate != 0) {
                            break;
                        }

                        if ((c2 & _Private_IonConstants.surrogate_mask) != _Private_IonConstants.low_surrogate_value) {
                            String message =
                                "Text contains unmatched UTF-16 high surrogate " +
                                IonTextUtils.printCodePointAsString(c);
                            throw new IonException(message);
                        }
                        c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                    }
                    else if ((c & _Private_IonConstants.surrogate_mask) == _Private_IonConstants.low_surrogate_value) {
                        String message =
                            "Text contains unmatched UTF-16 low surrogate " +
                            IonTextUtils.printCodePointAsString(c);
                        throw new IonException(message);
                    }
                    writeUnicodeScalarAsUTF8(c);
                }
            }

            if (c != terminator) {
                // TODO determine if this can really happen.
                throw new UnexpectedEofException();
            }
            if (debugValidation) _validate();

            return;
        }


        /*
         * this is a set of routines that put data into an Ion buffer
         * using the various type encodeing techniques
         *
         * these are the "high" level write routines (and read as well,
         * in the fullness of time)
         *
         *
         * methods with names of the form:
         *     void write<type>Value( value ... )
         * only write the value portion of the data
         *
         * methods with name like:
         *     int(len) write<type>WithLength( ... value ... )
         * write the trailing length and the value they return the
         * length written in case it is less than BB_SHORT_LEN_MAX (13)
         * so that the caller can backpatch the type descriptor if they
         * need to
         *
         * methods with names of the form:
         *     void write<type>( nibble, ..., value )
         * write the type descriptor the length if needed and the value
         *
         * methods with a name like:
         *     int <type>ValueLength( ... )
         * return the number of bytes that will be needed to write
         * the value out
         *
         */


        /**
         * Buffer for decoding (un)signed VarInt's and Int's
         */
        private byte[] numberBuffer = new byte[10];

        public int writeVarUIntValue(long value, boolean force_zero_write) throws IOException
        {
            assert value >= 0;
            int len = 0;
            if (value == 0) {
                if (force_zero_write) {
                    write((byte)0x80);
                    len = 1;
                }
            } else {
                int i = numberBuffer.length;
                // write every 7 bits of the value
                while (value > 0) {
                    numberBuffer[--i] = (byte)(value & 0x7f);
                    value = value >>> 7;
                }
                // set the end bit
                numberBuffer[numberBuffer.length - 1] |= 0x80;
                len = numberBuffer.length - i;
                write(numberBuffer, i, len);
            }
            return len;
        }

        /**
         * Writes a uint field of maximum length 8.
         * Note that this will write from the lowest to highest
         * order bits in the long value given.
         */
        public int writeUIntValue(long value) throws IOException
        {
            int i = numberBuffer.length;
            // even if value is Long.MIN_VALUE we will still serialize it correctly :)
            while (value != 0) {
                numberBuffer[--i] = (byte)(value & 0xff);
                value = value >>> 8;
            }
            int len = numberBuffer.length - i;
            write(numberBuffer, i, len);
            return len;
        }

        /**
         * Writes a uint field at given length
         * Note that this will write from the lowest to highest
         * order bits in the long value given.
         */
        public int writeUIntValue(long value, int len) throws IOException
        {
            int i = numberBuffer.length;
            for (int j = 0; j < len; ++j) {
                numberBuffer[--i] = (byte)(value & 0xff);
                value = value >>> 8;
            }
            write(numberBuffer, i, len);
            return len;
        }

        /**
         * Writes a uint field of arbitary length.  It does
         * check the value to see if a simpler routine can
         * handle the work;
         * Note that this will write from the lowest to highest
         * order bits in the long value given.
         */
        public int writeUIntValue(BigInteger value, int len) throws IOException
        {
            int returnlen = 0;
            int signum = value.signum();

            if (signum == 0) {
                // Zero has no bytes of data at all!  Nothing to write.
            }
            else if (signum < 0) {
                throw new IllegalArgumentException("value must be greater than or equal to 0");
            }
            else if (value.compareTo(MAX_LONG_VALUE) == -1) {
                long lvalue = value.longValue();
                returnlen = writeUIntValue(lvalue, len);
            }
            else {
                assert(signum > 0);
                byte[] bits = value.toByteArray();
                // BigInteger will pad this with a null byte sometimes
                // for negative numbers...let's skip past any leading null bytes
                int offset = 0;
                while (offset < bits.length && bits[offset] == 0) {
                    offset++;
                }
                int bitlen = bits.length - offset;
                this.write(bits, offset, bitlen);
                returnlen += bitlen;
            }
            assert(returnlen == len);
            return len;
        }


        public int writeVarIntValue(long value, boolean force_zero_write) throws IOException
        {
            int len = 0;
            if (value == 0) {
                if (force_zero_write) {
                    write((byte)0x80);
                    len = 1;
                }
            } else {
                int i = numberBuffer.length;
                boolean negative = value < 0;
                if (negative) {
                    value = -value;
                }
                // write every 7 bits of the value
                while (value > 0) {
                    numberBuffer[--i] = (byte)(value & 0x7f);
                    value = value >>> 7;
                }
                // set the end bit
                numberBuffer[numberBuffer.length - 1] |= 0x80;
                // increase the length of VarInt if the sign bit is 'occupied'
                // by the value to properly flag it
                if ((numberBuffer[i] & 0x40) == 0x40) {
                    numberBuffer[--i] = 0x00;
                }
                // set the sign bit
                if (negative) {
                    // add the sign bit to MSB
                    numberBuffer[i] |= 0x40;
                }
                len = numberBuffer.length - i;
                write(numberBuffer, i, len);
            }
            return len;
        }

        public int writeIntValue(long value) throws IOException {
            int i = numberBuffer.length;
            boolean negative = value < 0;
            if (negative) {
                value = -value;
            }
            while (value > 0) {
                numberBuffer[--i] = (byte)(value & 0xff);
                value = value >>> 8;
            }
            // increase the length of Int if the sign bit is 'occupied'
            // by the value to properly flag it
            if ((numberBuffer[i] & 0x80) == 0x40) {
                numberBuffer[--i] = 0x00;
            }
            // set the sign bit
            if (negative) {
                // add the sign bit to MSB
                numberBuffer[i] |= 0x80;
            }
            int len = numberBuffer.length - i;
            write(numberBuffer, i, len);
            return len;
        }
        public int writeFloatValue(double d) throws IOException
        {
            if (Double.valueOf(d).equals(DOUBLE_POS_ZERO))
            {
                // pos zero special case
                return 0;
            }

            // TODO write "custom" serialization or verify that
            //      the java routine is doing the right thing
            long dBits = Double.doubleToRawLongBits(d);
            return writeUIntValue(dBits, _ib_FLOAT64_LEN);
        }

        byte[] singleCodepointUtf8Buffer = new byte[4];
        public int writeUnicodeScalarAsUTF8(int c) throws IOException
        {
            int len;
            if (c < 0x80) {
                len = 1;
                this.start_write();
                _write((byte)(c & 0xff));
                this.end_write();
            } else {
                len = _writeUnicodeScalarToByteBuffer(c, singleCodepointUtf8Buffer, 0);
                this.write(singleCodepointUtf8Buffer, 0, len);
            }
            return len;
        }

        // this will write at least 2 byte unicode scalar to buffer
        private final int _writeUnicodeScalarToByteBuffer(int c, byte[] buffer, int offset) throws IOException
        {
            int len = -1;

            assert offset + 4 <= buffer.length;

            // first the quick, easy and common case - ascii
            if (c < 0x800) {
                // 2 bytes characters from 0x000080 to 0x0007FF
                buffer[offset] = (byte)( 0xff & (0xC0 | (c >> 6)) );
                buffer[++offset] = (byte)( 0xff & (0x80 | (c & 0x3F)) );
                len = 2;
            }
            else if (c < 0x10000) {
                // 3 byte characters from 0x800 to 0xFFFF
                // but only 0x800...0xD7FF and 0xE000...0xFFFF are valid
                if (c > 0xD7FF && c < 0xE000) {
                    this.throwUTF8Exception();
                }
                buffer[offset] = (byte)( 0xff & (0xE0 |  (c >> 12)) );
                buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) );
                buffer[++offset] = (byte)( 0xff & (0x80 |  (c & 0x3F)) );
                len = 3;
            }
            else if (c <= 0x10FFFF) {
                // 4 byte characters 0x010000 to 0x10FFFF
                // these are are valid
                buffer[offset] = (byte)( 0xff & (0xF0 |  (c >> 18)) );
                buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 12) & 0x3F)) );
                buffer[++offset] = (byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) );
                buffer[++offset] = (byte)( 0xff & (0x80 | (c & 0x3F)) );
                len = 4;
            }
            else {
                this.throwUTF8Exception();
            }
            return len;
        }

        /******************************
         *
         * These are the "write typed value with header" routines
         * they all are of the form:
         *
         *     void write<type>(nibble, ...)
         */
        public int writeByte(HighNibble hn, int len) throws IOException
        {
            if (len < 0) {
                throw new IonException("negative token length encountered");
            }
            if (len > 13) len = 14; // TODO remove magic numbers
            int t = _Private_IonConstants.makeTypeDescriptor( hn.value(), len );
            write(t);
            return 1;
        }

        /**
         * Write one byte.
         *
         * @param b the byte to write.
         * @return the number of bytes written (always 1).
         * @throws IOException
         */
        public int writeByte(byte b) throws IOException
        {
            write(b);
            return 1;
        }

        /**
         * Write one byte.
         *
         * @param b integer containing the byte to write; only the 8 low-order
         * bits are written.
         *
         * @return the number of bytes written (always 1).
         * @throws IOException
         */
        public int writeByte(int b) throws IOException
        {
            write(b);
            return 1;
        }

        public int writeAnnotations(SymbolToken[] annotations,
                                    SymbolTable symbolTable) throws IOException
        {
            int startPosition = this.position();

            int annotationLen = 0;
            for (int ii=0; ii<annotations.length; ii++) {
                int sid = annotations[ii].getSid();
                assert sid != SymbolTable.UNKNOWN_SYMBOL_ID;
                annotationLen += IonBinary.lenVarUInt(sid);
            }

            // write the len of the list
            this.writeVarUIntValue(annotationLen, true);

            // write the symbol id's
            for (int ii=0; ii<annotations.length; ii++) {
                int sid = annotations[ii].getSid();
                this.writeVarUIntValue(sid, true);
            }

            return this.position() - startPosition;
        }

        public int writeAnnotations(ArrayList<Integer> annotations)
            throws IOException
        {
            int startPosition = this.position();

            int annotationLen = 0;
            for (Integer ii : annotations) {
                annotationLen += IonBinary.lenVarUInt(ii.intValue());
            }

            // write the len of the list
            this.writeVarUIntValue(annotationLen, true);

            // write the symbol id's
            for (Integer ii : annotations) {
                this.writeVarUIntValue(ii.intValue(), true);
            }

            return this.position() - startPosition;
        }


        public void writeStubStructHeader(int hn, int ln)
            throws IOException
        {
            // write the hn and ln as the typedesc, we'll patch it later.
            writeByte(_Private_IonConstants.makeTypeDescriptor(hn, ln));
        }

        /**
         * Write typedesc and length for the common case.
         */
        public int writeCommonHeader(int hn, int len)
            throws IOException
        {
            int returnlen = 0;

            // write then len in low nibble
            if (len < _Private_IonConstants.lnIsVarLen) {
                returnlen += writeByte(_Private_IonConstants.makeTypeDescriptor(hn, len));
            }
            else {
                returnlen += writeByte(_Private_IonConstants.makeTypeDescriptor(hn, _Private_IonConstants.lnIsVarLen));
                returnlen += writeVarUIntValue(len, false);
            }
            return returnlen;
        }


        /***************************************
         *
         * Here are the write type value with type descriptor and everything
         * family of methods, these depend on the others to do much of the work
         *
         */

        /**
         * @param sid must be valid id (>=1)
         */
        public int writeSymbolWithTD(int sid) // was: (String s, SymbolTable symtab)
            throws IOException
        {
            // was: int sid = symtab.addSymbol(s);
            assert sid > 0;

            int vlen = lenUInt(sid);
            int len = this.writeCommonHeader(
                                 _Private_IonConstants.tidSymbol
                                ,vlen
                           );
            len += this.writeUIntValue(sid, vlen);

            return len;
        }


        /**
         * Writes the full string header + data.
         */
        public int writeStringWithTD(String s) throws IOException
        {
            // first we have to see how long this will be in the output
            /// buffer - part of the cost of length prefixed values
            int len = IonBinary.lenIonString(s);
            if (len < 0) this.throwUTF8Exception();

            // first we write the type desc and length
            len += this.writeCommonHeader(_Private_IonConstants.tidString, len);

            // now we write just the value out
            writeStringData(s);
            //for (int ii=0; ii<s.length(); ii++) {
            //    char c = s.charAt(ii);
            //    this.writeCharValue(c);
            //}

            return len;
        }

        static final int stringBufferLen = 128;
        byte[] stringBuffer = new byte[stringBufferLen];
        public int writeStringData(String s) throws IOException
        {
            int len = 0;
            int bufPos = 0;
            for (int ii=0; ii<s.length(); ii++) {
                int c = s.charAt(ii);
                if (bufPos > stringBufferLen - 4) { // 4 is the max UTF-8 encoding size
                    this.write(stringBuffer, 0, bufPos);
                    bufPos = 0;
                }
                // at this point stringBuffer contains enough space for UTF-8 encoded code point
                if (c < 128) {
                    // don't even both to call the "utf8" converter for ascii
                    stringBuffer[bufPos++] = (byte)c;
                    len++;
                    continue;
                }
                // multi-byte utf8

                if (c >= 0xD800 && c <= 0xDFFF) {
                    if (_Private_IonConstants.isHighSurrogate(c)) {
                        // houston we have a high surrogate (let's hope it has a partner
                        if (++ii >= s.length()) {
                            throw new IllegalArgumentException("invalid string, unpaired high surrogate character");
                        }
                        int c2 = s.charAt(ii);
                        if (!_Private_IonConstants.isLowSurrogate(c2)) {
                            throw new IllegalArgumentException("invalid string, unpaired high surrogate character");
                        }
                        c = _Private_IonConstants.makeUnicodeScalar(c, c2);
                    }
                    else if (_Private_IonConstants.isLowSurrogate(c)) {
                        // it's a loner low surrogate - that's an error
                        throw new IllegalArgumentException("invalid string, unpaired low surrogate character");
                    }
                    // from 0xE000 up the _writeUnicodeScalar will check for us
                }
                int utf8len = this._writeUnicodeScalarToByteBuffer(c, stringBuffer, bufPos);
                bufPos += utf8len;
                len += utf8len;
            }
            if (bufPos > 0) {
                this.write(stringBuffer, 0, bufPos);
            }

            return len;
        }

        public int writeNullWithTD(HighNibble hn) throws IOException
        {
            writeByte(hn, _Private_IonConstants.lnIsNullAtom);
            return 1;
        }

        public int writeTimestampWithTD(Timestamp di)
            throws IOException
        {
            int  returnlen;

            if (di == null) {
                returnlen = this.writeCommonHeader(
                                                   _Private_IonConstants.tidTimestamp
                                                   ,_Private_IonConstants.lnIsNullAtom);
            }
            else {
                int vlen = IonBinary.lenIonTimestamp(di);

                returnlen = this.writeCommonHeader(
                                                   _Private_IonConstants.tidTimestamp
                                                   ,vlen);

                int wroteLen = writeTimestamp(di);
                assert wroteLen == vlen;
                returnlen += wroteLen;
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
                returnlen += this.writeVarIntValue(offset.intValue(), true);
            }

            // now the date - year, month, day as VarUInts
            // if we have a non-null value we have at least the date
            if (precision.includes(Precision.YEAR)) {
                returnlen += this.writeVarUIntValue(di.getZYear(), true);
            }
            if (precision.includes(Precision.MONTH)) {
                returnlen += this.writeVarUIntValue(di.getZMonth(), true);
            }
            if (precision.includes(Precision.DAY)) {
                returnlen += this.writeVarUIntValue(di.getZDay(), true);
            }

            // now the time portion
            if (precision.includes(Precision.MINUTE)) {
                returnlen += this.writeVarUIntValue(di.getZHour(), true);
                returnlen += this.writeVarUIntValue(di.getZMinute(), true);
            }
            if (precision.includes(Precision.SECOND)) {
                returnlen += this.writeVarUIntValue(di.getZSecond(), true);
                // and, finally, any fractional component that is known
                BigDecimal fraction = di.getZFractionalSecond();
                if (fraction != null) {
                    assert !fraction.equals(BigDecimal.ZERO);
                    returnlen += this.writeDecimalContent(fraction);
                }
            }
            return returnlen;
        }

        public int writeDecimalWithTD(BigDecimal bd) throws IOException
        {
            int returnlen;

            // we only write out the '0' value as the nibble 0
            if (bd == null) {
                returnlen =
                    this.writeByte(NULL_DECIMAL_TYPEDESC);
            }
            else if (isNibbleZero(bd)) {
                returnlen =
                    this.writeByte(ZERO_DECIMAL_TYPEDESC);
            }
            else {
                // otherwise we to it the hard way ....
                int len = IonBinary.lenIonDecimal(bd);

                if (len < _Private_IonConstants.lnIsVarLen) {
                    returnlen = this.writeByte(
                            _Private_IonConstants.makeTypeDescriptor(
                                    _Private_IonConstants.tidDecimal
                                    , len
                            )
                        );
                }
                else {
                    returnlen = this.writeByte(
                            _Private_IonConstants.makeTypeDescriptor(
                                    _Private_IonConstants.tidDecimal
                                    , _Private_IonConstants.lnIsVarLen
                            )
                        );
                    this.writeVarIntValue(len, false);
                }
                int wroteDecimalLen = writeDecimalContent(bd);
                assert wroteDecimalLen == len;
                returnlen += wroteDecimalLen;
            }
            return returnlen;
        }

        private static final byte[] negativeZeroBitArray = new byte[] { (byte)0x80 };

        /** Zero-length byte array. */
        private static final byte[] positiveZeroBitArray = EMPTY_BYTE_ARRAY;


        /**
         * @see com.amazon.ion.impl.lite.ReverseBinaryEncoder#writeIonDecimalContent
         */
        public int writeDecimalContent(BigDecimal bd)
            throws IOException
        {
            // check for null and 0. which are encoded in the nibble itself.
            if (bd == null) return 0;

            if (isNibbleZero(bd)) return 0;

            // Ion stores exponent, BigDecimal uses the negation "scale"
            int exponent = -bd.scale();

            // The exponent isn't optional (except for the 0d0 case above).
            int returnlen = writeVarIntValue(exponent,
                                             /* force_zero_write*/ true);

            BigInteger mantissa = bd.unscaledValue();

            byte[] mantissaBits;
            switch (mantissa.signum()) {
            case 0:
                if (Decimal.isNegativeZero(bd)) {
                    mantissaBits = negativeZeroBitArray;
                }
                else {
                    mantissaBits = positiveZeroBitArray;
                }
                break;
            case -1:
                // Obtain the unsigned value of the BigInteger
                // We cannot use the twos complement representation of a
                // negative BigInteger as this is different from the encoding
                // of basic field Int.
                mantissaBits = mantissa.negate().toByteArray();
                // Set the sign on the highest order bit of the first octet
                mantissaBits[0] |= 0x80;
                break;
            case 1:
                mantissaBits = mantissa.toByteArray();
                break;
            default:
                throw new IllegalStateException("mantissa signum out of range");
            }

            this.write(mantissaBits, 0, mantissaBits.length);
            returnlen += mantissaBits.length;

            return returnlen;
        }


        void throwUTF8Exception()
        {
            throwException("Invalid UTF-8 character encounter in a string at pos " + this.position());
        }

        void throwException(String s)
        {
            throw new BlockedBuffer.BlockedBufferException(s);
        }
    }

    public static class PositionMarker
    {
        int     _pos;
        Object  _userData;

        public PositionMarker() {}
        public PositionMarker(int pos, Object o) {
            _pos = pos;
            _userData = o;
        }

        public int getPosition()       { return _pos; }
        public Object getUserData()    { return _userData; }

        public void setPosition(int pos) {
            _pos = pos;
        }
        public void setUserData(Object o) {
            _userData = o;
        }
    }
}
