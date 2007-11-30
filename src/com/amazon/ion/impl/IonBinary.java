/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonConstants.BINARY_VERSION_MARKER_1_0;
import static com.amazon.ion.impl.IonConstants.BINARY_VERSION_MARKER_SIZE;
import com.amazon.ion.IonException;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.UnexpectedEofException;
import com.amazon.ion.impl.IonConstants.HighNibble;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Date;
import java.util.Stack;


/**
 *
 */
public class IonBinary
{
    static boolean debugValidation = false;

    final static int _ib_TOKEN_LEN           =    1;
    final static int _ib_VAR_INT32_LEN_MAX   =    5; // 31 bits (java limit) / 7 bits per byte = 5 bytes
    final static int _ib_VAR_INT64_LEN_MAX   =   10; // 31 bits (java limit) / 7 bits per byte = 5 bytes
    final static int _ib_INT64_LEN_MAX       =    8;
    final static int _ib_FLOAT64_LEN         =    8;

    private static final Double DOUBLE_POS_ZERO = Double.valueOf(0.0);

    private IonBinary() { }

    public static boolean startsWithBinaryVersionMarker(byte[] b)
    {
        if (b.length < BINARY_VERSION_MARKER_SIZE) return false;

        for (int i = 0; i < BINARY_VERSION_MARKER_SIZE; i++)
        {
            if (BINARY_VERSION_MARKER_1_0[i] != b[i]) return false;
        }
        return true;
    }

    /**
     * Verifies that a reader starts with a valid Ion cookie, throwing an
     * exception if it does not.
     *
     * @param reader must not be null.
     * @throws IonException if there's a problem reading the cookie, or if the
     * data does not start with {@link IonConstants#BINARY_VERSION_MARKER_1_0}.
     */
    public static void verifyBinaryVersionMarker(Reader reader)
        throws IonException
    {
        try
        {
            reader.sync();
            reader.setPosition(0);
            byte[] bvm = new byte[BINARY_VERSION_MARKER_SIZE];
            int len = reader.read(bvm);
            if (len < BINARY_VERSION_MARKER_SIZE)
            {
                String message =
                    "Binary data is too short: at least " +
                    BINARY_VERSION_MARKER_SIZE +
                    " bytes are required, but only " + len + " were found.";
                throw new IonException(message);

            }

            if (! startsWithBinaryVersionMarker(bvm))
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
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
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
         * creates a blocked byte buffer from an input stream.
         * @param bytestream a stream interface the byte image to buffer
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
    public static int lenVarUInt7(int intVal) {  // we write a lot of these
        int len = 0;

        if (intVal != 0) {
            if (intVal < 0) {
                throw new BlockedBuffer.BlockedBufferException("fatal signed long where unsigned was promised");
            }

            len = _ib_VAR_INT32_LEN_MAX - 1;
            while ( ( 0x7f & (intVal>>(7*len)) ) == 0 ) {
                len--;
            }
            if (len < 0) len = 0;
            len++;
        }
        return len;
    }
    public static int lenVarUInt7(long longVal) {
        int len = 0;

        if (longVal != 0) {
            if (longVal < 0) {
                throw new BlockedBuffer.BlockedBufferException("fatal signed long where unsigned was promised");
            }

            len = _ib_VAR_INT64_LEN_MAX - 1;
            while ( (int)( 0x7fL & (longVal>>(7*len)) ) == 0 ) {
                len--;
            }
            if (len < 0) len = 0;
            len++;
        }
        return len;
    }
    public static int lenVarUInt8(long longVal) {
        int len = 0;

        // figure out how many we have bytes we have to write out
        if (longVal != 0) {
            if (longVal < 0) {
                throw new BlockedBuffer.BlockedBufferException("fatal signed long where unsigned was promised");
            }

            len = _ib_INT64_LEN_MAX - 1;
            while ( (int)( 0xffL & (longVal>>(8*len)) ) == 0 ) {
                len--;
            }
            if (len < 0) {
                // "toast until it smokes" -- Piet Hein
                len = 0;
            }
            len++;
        }
        return len;
    }


    public static int lenVarInt8(BigInteger bi)
    {
        if (bi.compareTo(BigInteger.ZERO) < 0)
        {
            // TODO avoid negate call? (maybe slow)
            bi = bi.negate();

            // Here's why its hard to avoid negate:
//          assert (new BigInteger("-2").bitLength()) == 1;
            // We need 2 bits to represent the magnitude.
        }

        int bitCount = bi.bitLength() + 1;   // One more bit to hold the sign

        int byteCount = (bitCount + 7) / 8;
        return byteCount;
    }


    // TODO maybe add lenVarInt7(int) to micro-optimize

    public static int lenVarInt7(long longVal) {
        int len = 0;

        // figure out how many we have bytes we have to write out
        if (longVal != 0) {

            len = _ib_VAR_INT64_LEN_MAX - 1;
            if (longVal < 0) {
                longVal = -longVal;
            }
            while ( (int)( 0x7fL & (longVal>>(7*len)) ) == 0 ) {
                len--;
            }
            if (((~0x3fL) & (longVal >> (7*len))) != 0) {
                len++;
            }
            if (len < 0) {
                len = 0;
            }
            len++;  // 0 based for shift, 1 based for return length
        }
        return len;
    }
    public static int lenIonInt(long v) {
        if (v < 0) {
            return IonBinary.lenVarUInt8(-v);
        }
        else if (v > 0) {
            return IonBinary.lenVarUInt8(v);
        }
        return 0; // CAS UPDATE, was 1
    }
    public static int lenVarInt(long longVal) {
        int len = 0;

        // figure out how many we have bytes we have to write out
        if (longVal != 0) {
            len = _ib_INT64_LEN_MAX - 1;
            if (longVal < 0) {
                longVal = -longVal;
            }
            while ( (int)( 0xffL & (longVal >> (8*len)) ) == 0 ) {
                len--;
            }
            if (((~0x7fL) & (longVal >> (8*len))) != 0) {
                len++;
            }
            len++;
        }
        return len;
    }

    /**
     * The size of length value when short lengths are recorded in the
     * typedesc low-nibble.
     * @param valuelen
     * @return zero if valuelen < 14
     */
    public static int lenLenFieldWithOptionalNibble(int valuelen) {
        if (valuelen < IonConstants.lnIsVarLen) {
            return 0;
        }
        return lenVarUInt7(valuelen);
    }

    public static int lenTypeDescWithAppropriateLenField(int type, int valuelen)
    {
        switch (type) {
        case IonConstants.tidNull: // null(0)
        case IonConstants.tidBoolean: // boolean(1)
            return IonConstants.BB_TOKEN_LEN;

        case IonConstants.tidPosInt: // 2
        case IonConstants.tidNegInt: // 3
        case IonConstants.tidFloat: // float(4)
        case IonConstants.tidDecimal: // decimal(5)
        case IonConstants.tidTimestamp: // timestamp(6)
        case IonConstants.tidSymbol: // symbol(7)
        case IonConstants.tidString: // string (8)
        case IonConstants.tidClob: // clob(9)
        case IonConstants.tidBlob: // blob(10)
        case IonConstants.tidTypedecl: // 14
            if (valuelen < IonConstants.lnIsVarLen) {
                return IonConstants.BB_TOKEN_LEN;
            }
            // fall through since the len has to follow
            // the td byte, just like it does in containers
        case IonConstants.tidList:   // 11
        case IonConstants.tidSexp:   // 12
        case IonConstants.tidStruct: // 13
            return IonConstants.BB_TOKEN_LEN + lenVarUInt7(valuelen);

        case IonConstants.tidUnused: // unused(15)
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
     * @param bd must not be null.
     */
    public static int lenIonDecimal(BigDecimal bd) {
        int len = 0;
        // first check for the 0d0 special case
        if (!BigDecimal.ZERO.equals(bd)) {
            // otherwise this is very expensive (or so I'd bet)
            BigInteger mantissa = bd.unscaledValue();
            int mantissaByteCount = lenVarInt8(mantissa);

            // We really need the length of the exponent (-scale) but in our
            // representation the length is the same regardless of sign.
            int scale = bd.scale();
            len = lenVarInt7(scale);

            // Exponent is always at least one byte.
            if (len == 0) len = 1;

            len += mantissaByteCount;
        }
        return len;
    }


    public static int lenIonTimestamp(IonTokenReader.Type.timeinfo di)
    {
        if (di == null) return 0;

        long l = di.d.getTime();
        BigDecimal bd = new BigDecimal(l);
        bd.setScale(13); // millisecond time has 13 significant digits

        int  tzoffset = (di.localOffset == null) ? 0 : di.localOffset.intValue();

        int  tzlen = IonBinary.lenVarInt7(tzoffset);
        if (tzlen == 0) tzlen = 1;
        int  bdlen = IonBinary.lenIonDecimal(bd);

        return tzlen + bdlen;
    }
    public static int lenIonString(String v) {
        if (v == null) return 0;

        int len = 0;

        for (int ii=0; ii<v.length(); ii++) {
            char c = v.charAt(ii);
            int clen = lenUTF8Char(c);
            if (clen < 1) return -1;
            len += clen;
        }

        return len;
    }
    public static int lenUTF8Char(int c) {
        int len = -1;

        if ((c & (~0x1FFFFF)) != 0) {
            throw new IonException("invalid character for UTF-8 output");
        }

        if ((c & (~0x7f)) == 0) {
            len = 1;
        }
        else if ((c & (~0x7ff)) == 0) {
            len  = 2;
        }
        else if ((c & (~0xffff)) == 0) {
            len  = 3;
        }
        else if ((c & (~0x7ffff)) == 0) {
            len  = 4;
        }

        return len;
    }

    public static int lenAnnotationListWithLen(String[] annotations, LocalSymbolTable symbolTable)
    {
        int annotationLen = 0;

        if (annotations != null) {
            // add up the length of the encoded symbols
            for (int ii=0; ii<annotations.length; ii++) {
                int symid = symbolTable.findSymbol(annotations[ii]);
                annotationLen += IonBinary.lenVarUInt7(symid);
            }

            // now the len of the list
            annotationLen += IonBinary.lenVarUInt7(annotationLen);
        }
        return annotationLen;
    }

    public static int lenAnnotationListWithLen(ArrayList<Integer> annotations)
    {
        int annotationLen = 0;

        // add up the length of the encoded symbols
        for (Integer ii : annotations) {
            int symid = ii.intValue();
            annotationLen += IonBinary.lenVarUInt7(symid);
        }

        // now the len of the list
        annotationLen += IonBinary.lenVarUInt7(annotationLen);

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
            int vlen;
            if (vl < 0) {
                vlen = lenVarInt(vl);
            }
            else {
                vlen = lenVarUInt8(vl);
            }
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
    public static int lenIonTimestampWithTypeDesc(IonTokenReader.Type.timeinfo di) {
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
            int typeid = IonConstants.getTypeCode(c);
            if (typeid == IonConstants.tidTypedecl) {
                this.readLength(typeid, IonConstants.getLowNibble(c));
                int alen = this.readVarInt7IntValue();
                // TODO add skip(int) method instead of this loop.
                while (alen > 0) {
                    if (this.read() < 0) throwUnexpectedEOFException();
                    alen--;
                }
                c = read();
                if (c < 0) throwUnexpectedEOFException();
            }
            return c;
        }

        public int[] readAnnotations() throws IOException
        {
            int[] annotations = null;

            int annotationLen = this.readVarUInt7IntValue();
            int annotationPos = this.position(); // pos at the first ann sid
            int annotationEnd = annotationPos + annotationLen;
            int annotationCount = 0;

            // first we read through and count
            while(this.position() < annotationEnd) {
                // read the annotation symbol id itself
                // and for this first pass we just throw that
                // value away, since we're just counting
                this.readVarUInt7IntValue();
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
                    int sid = this.readVarUInt7IntValue();
                    annotations[annotationIdx++] = sid;
                }
            }

            return annotations;
        }

        public String[] readAnnotations(LocalSymbolTable symbolTable) throws IOException
        {
            String[] annotations = null;

            int annotationLen = this.readVarUInt7IntValue();
            int annotationPos = this.position(); // pos at the first ann sid
            int annotationEnd = annotationPos + annotationLen;
            int annotationCount = 0;

            // first we read through and count
            while(this.position() < annotationEnd) {
                // read the annotation symbol id itself
                // and for this first pass we just throw that
                // value away, since we're just counting
                this.readVarUInt7IntValue();
                annotationCount++;
            }
            if (annotationCount > 0) {
                // then, if there are any there, we
                // allocate the array, and re-read the sids
                // look them up and fill in the array
                annotations = new String[annotationCount];
                int annotationIdx = 0;
                this.setPosition(annotationPos);
                while(this.position() < annotationEnd) {
                    // read the annotation symbol id itself
                    int sid = this.readVarUInt7IntValue();
                    annotations[annotationIdx++] = symbolTable.findSymbol(sid);
                }
            }

            return annotations;
        }

        public int readLength(int td, int ln) throws IOException
        {
            // TODO check for invalid lownibbles
            switch (td) {
            case IonConstants.tidNull: // null(0)
            case IonConstants.tidBoolean: // boolean(1)
                return 0;
            case IonConstants.tidPosInt: // 2
            case IonConstants.tidNegInt: // 3
            case IonConstants.tidFloat: // float(4)
            case IonConstants.tidDecimal: // decimal(5)
            case IonConstants.tidTimestamp: // timestamp(6)
            case IonConstants.tidSymbol: // symbol(7)
            case IonConstants.tidString: // string (8)
            case IonConstants.tidClob: // clob(9)
            case IonConstants.tidBlob: // blob(10)
            case IonConstants.tidList:     // 11
            case IonConstants.tidSexp:     // 12
            case IonConstants.tidTypedecl: // 14
                switch (ln) {
                case 0:
                case IonConstants.lnIsNullAtom:
                    return 0;
                case IonConstants.lnIsVarLen:
                    return readVarUInt7IntValue();
                default:
                    return ln;
                }
            case IonConstants.tidStruct: // 13
                switch (ln) {
                case IonConstants.lnIsEmptyContainer:
                case IonConstants.lnIsNullStruct:
                    return 0;
                case IonConstants.lnIsOrderedStruct:
                case IonConstants.lnIsVarLen:
                    return readVarUInt7IntValue();
                default:
                    return ln;
                }
            case IonConstants.tidUnused: // unused(15)
            default:
                // TODO use InvalidBinaryDataException
                throw new BlockedBuffer.BlockedBufferException("invalid type id encountered: " + td);
            }
        }

        public long readFixedIntLongValue(int len) throws IOException {
            int retvalue = 0;
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
        public long readVarInt8LongValue(int len) throws IOException {
            long    retvalue = 0;
            boolean is_negative = false;
            int     b;

            if (len > 0) {
                // read the first byte
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (b & 0x7F);
                is_negative = ((b & 0x80) != 0);

                switch (len - 1) {  // we read 1 already
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
                    retvalue = -retvalue;
                }
            }
            return retvalue;
        }
        /** @throws IOException
         * @deprecated */
        @Deprecated
        public int readVarInt8IntValue(int len) throws IOException {
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
                    retvalue = -retvalue;
                }
            }
            return retvalue;
        }
        public long readVarUInt8LongValue(int len) throws IOException {
            long    retvalue = 0;
            int b;

            switch (len) {
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
            default:
            }
            return retvalue;
        }
        public int readVarUInt8IntValue(int len) throws IOException {
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
        public long readVarUInt7LongValue() throws IOException {
            long retvalue = 0;
            int  b;

            for (;;) {
                if ((b = read()) < 0) throwUnexpectedEOFException();
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break;
            }
            return retvalue;
        }
        public int readVarUInt7IntValue() throws IOException {
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
        public long readVarInt7LongValue() throws IOException
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
                    retvalue = (retvalue << 7) | (b & 0x7F);
                    if ((b & 0x80) != 0) break done;
                }
            }
            if (is_negative) {
                retvalue = -retvalue;
            }
            return retvalue;
        }
        public int readVarInt7IntValue() throws IOException
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
                retvalue = -retvalue;
            }
            return retvalue;
        }

        /**
         * Reads an integer value, returning null to mean -0.
         * @throws IOException
         */
        public Integer readVarInt7WithNegativeZero() throws IOException
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
                    retInteger = new Integer(-retvalue);
                }
            }
            else {
                retInteger = new Integer(retvalue);
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

            long dBits = readVarUInt8LongValue(len);
            return Double.longBitsToDouble(dBits);
        }
        public BigDecimal readDecimalValue(int len) throws IOException
        {
            BigDecimal bd;

            // we only write out the '0' value as the nibble 0
            if (len == 0) {
                bd = new BigDecimal(0, MathContext.DECIMAL128);
            }
            else {
                // otherwise we to it the hard way ....
                int         startpos = this.position();
                int         exponent = this.readVarInt7IntValue();
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
        public IonTokenReader.Type.timeinfo readTimestampValue(int len) throws IOException
        {
            if (len < 1) return null;
            int startpos = this.position();

            // first the timezone, and -0 is unknown
            Integer tz = this.readVarInt7WithNegativeZero();

            // now the time part
            BigDecimal bd = this.readDecimalValue(len - (this.position() - startpos));

            // now we put it together
            IonTokenReader.Type.timeinfo ti = new IonTokenReader.Type.timeinfo();
            ti.d = new Date(bd.longValue());
            ti.localOffset = tz;

            return ti;
        }

        public String readString(int len) throws IOException
        {
            StringBuffer sb = new StringBuffer(len);
            int c;
            int endPosition = this.position() + len;

            while (this.position() < endPosition) {
                c = readChar();
                if (c < 0) throwUnexpectedEOFException();
                sb.append((char)c);
            }

            if (this.position() < endPosition) throwUnexpectedEOFException();

            return sb.toString();
        }
        public int readChar() throws IOException {
            int c = -1, b;

            b = read();
            if (b < 0) return -1;
            if ((b & 0x80) == 0) {
                return b;
            }

            if ((b & 0xe0) == 0xc0) {
                c = (b & ~0xe0);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
            }
            else if ((b & 0xf0) == 0xe0) {
                c = (b & ~0xf0);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
                b = read();
                if ((b & 0xc0) != 0x80) throwUTF8Exception();
                c <<= 6;
                c |= (b & ~0x80);
            }
            else if ((b & 0xf8) == 0xf0) {
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
            if (IonConstants.getTypeCode(td) != IonConstants.tidString) {
                throw new IonException("readString helper only works for string(7) not "+((td >> 4 & 0xf)));
            }
            int len = (td & 0xf);
            if (len == IonConstants.lnIsNullAtom) {
                return null;
            }
            else if (len == IonConstants.lnIsVarLen) {
                len = this.readVarUInt7IntValue();
            }
            return readString(len);
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
        public void pushPosition(Object o)
        {
            PositionMarker pm = new PositionMarker(this.position(), o);
            if (_pos_stack == null) {
                _pos_stack = new Stack<PositionMarker>();
            }
            _pos_stack.push(pm);
        }
        public PositionMarker popPosition()
        {
            PositionMarker pm = _pos_stack.pop();
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
           PositionMarker pm = this.popPosition();
           lhNode n = (lhNode)pm.getUserData();
           int totallen = currpos - pm.getPosition();

           // fix up the low nibble if we need to
           if (lownibble == -1) {
               lownibble = n._lownibble;
           }

           // calculate the length just the value itself
           // we don't count the type descriptor in the value len
           int writtenValueLen = totallen - IonConstants.BB_TOKEN_LEN;

           // now we can figure out how long the value is going to be

           // This is the length of the length (it does NOT
           // count the typedesc byte however)
           int len_o_len = IonBinary.lenVarUInt7(writtenValueLen);

           // TODO cleanup this logic.  lengthFollows == is struct
           if (n._length_follows) {
               assert hn == IonConstants.tidStruct;

               if (lownibble == IonConstants.lnIsOrderedStruct)
               {
                   // leave len_o_len alone
               }
               else
               {
                   if (writtenValueLen < IonConstants.lnIsVarLen) {
                       lownibble = writtenValueLen;
                       len_o_len = 0;
                   }
                   else {
                       lownibble = IonConstants.lnIsVarLen;
                   }
                   assert lownibble != IonConstants.lnIsOrderedStruct;
               }
           }
           else {
               if (writtenValueLen < IonConstants.lnIsVarLen) {
                   lownibble = writtenValueLen;
                   len_o_len = 0;
               }
               else {
                   lownibble = IonConstants.lnIsVarLen;
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
           this.writeByte(IonConstants.makeTypeDescriptor(hn, lownibble));
           if (len_o_len > 0) {
               this.writeVarUInt7Value(writtenValueLen, true);
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

        public void appendToLongValue(CharSequence chars, boolean onlyByteSizedCharacters) throws IOException
        {
            if (debugValidation) _validate();

            int len = chars.length();

            for (int ii = 0; ii < len; ii++)
            {
                char c = chars.charAt(ii);
                if (onlyByteSizedCharacters && (c > 255)) {
                    throw new IonException("escaped character value too large in clob (0 to 255 only)");
                }
                writeCharValue(c);
            }

            if (debugValidation) _validate();
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
        @SuppressWarnings("deprecation")
        public void appendToLongValue(int terminator
                                     ,boolean longstring
                                     ,boolean onlyByteSizedCharacters
                                     ,PushbackReader r
                                     )
           throws IOException, UnexpectedEofException
        {
           int c;

           if (debugValidation) _validate();

           for (;;) {
               c = r.read();
               if (c == terminator) {
                   if (!longstring) {
                       // if it's not a long string one quote is enough
                       break;
                   }
                   // otherwise we need two more to really be done
                   c = r.read();
                   if (c == terminator) {
                       c = r.read();
                       if (c == terminator) {
                           // now we're done, this string is closed
                           break;
                       }
                       // otherwise we have another terminator to put back
                       write((byte)(0xff & terminator)); // terminators are always ascii characters, like '
                   }
                   // write the terminator we already read past
                   write((byte)(0xff & terminator));
               }
               if (c == -1) {
                   throw new UnexpectedEofException();
               }
               if (terminator != -1) {
                   if (!longstring && (c == '\n' || c == '\r')) {
                       throw new IonException("unexpected line terminator encountered in quoted string");
                   }
                   if (c == '\\') {
                       c = IonTokenReader.readEscapedCharacter(r);
                       // EOF throws UnexpectedEofException
                   }
                   // TODO can this move up into prior if?
                   if (c != IonTokenReader.EMPTY_ESCAPE_SEQUENCE) {
                       if (onlyByteSizedCharacters) {
                           if ((c & (~0xff)) != 0) {
                               throw new IonException("escaped character value too large in clob (0 to 255 only)");
                           }
                           write((byte)(0xff & c));
                       }
                       else {
                           writeCharValue(c);
                       }
                   }
               }
               else {
                   if (onlyByteSizedCharacters) {
                       if ((c & (~0xff)) != 0) {
                           throw new IonException("escaped character value too large in clob (0 to 255 only)");
                       }
                       write((byte)(0xff & c));
                   }
                   else {
                       writeCharValue(c);
                   }
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


        /**************************
         *
         * These are the "write value" family, they just write the value
         * @throws IOException
         */
        public int writeFixedIntValue(long val, int len) throws IOException
        {
            switch (len) {
            case 8: write((byte)((val >> 56) & 0xffL));
            case 7: write((byte)((val >> 48) & 0xffL));
            case 6: write((byte)((val >> 40) & 0xffL));
            case 5: write((byte)((val >> 32) & 0xffL));
            case 4: write((byte)((val >> 24) & 0xffL));
            case 3: write((byte)((val >> 16) & 0xffL));
            case 2: write((byte)((val >>  8) & 0xffL));
            case 1: write((byte)((val >>  0) & 0xffL));
            }
            return len;
        }
        public int writeVarUInt7Value(int value, int fixed_size) throws IOException
        {
            int mask = 0x7F;
            int len = lenVarUInt7(value);

            if (fixed_size > 0) {
                if (fixed_size < len) {
                    throwException(
                             "VarUInt7 overflow, fixed size ("
                            +fixed_size
                            +") too small for value ("
                            +value
                            +")"
                    );
                }
                len = fixed_size;
            }

            switch (len-1) {
            case 4: write((byte)((value >> (7*4)) & mask));
            case 3: write((byte)((value >> (7*3)) & mask));
            case 2: write((byte)((value >> (7*2)) & mask));
            case 1: write((byte)((value >> (7*1)) & mask));
            case 0: write((byte)((value & mask) | 0x80));
                    break;
            }
            return len;
        }
        public int writeVarUInt7Value(int value, boolean force_zero_write) throws IOException
        {
            int mask = 0x7F;
            int len = lenVarUInt7(value);

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

        /**
         * Writes a uint field of maximum length 8.
         * Note that this will write from the lowest to highest
         * order bits in the long value given.
         */
        public int writeVarUInt8Value(long value, int len) throws IOException
        {
            long mask = 0xffL;

            switch (len) {
            case 8: write((byte)((value >> (56)) & mask));
            case 7: write((byte)((value >> (48)) & mask));
            case 6: write((byte)((value >> (40)) & mask));
            case 5: write((byte)((value >> (32)) & mask));
            case 4: write((byte)((value >> (24)) & mask));
            case 3: write((byte)((value >> (16)) & mask));
            case 2: write((byte)((value >> (8)) & mask));
            case 1: write((byte)(value & mask));
                    break;
            }
            return len;
        }

        public int writeVarInt7Value(int value, boolean force_zero_write) throws IOException
        {
            int len = 0;

            if (value != 0) {
                int mask = 0x7F;
                boolean is_negative = false;

                len = lenVarInt7(value);
                if (is_negative = (value < 0)) {
                    value = -value;
                }

                // we write the first "byte" separately as it has the sign
                int b = (byte)((value >> (7*(len-1))) & mask);
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
                len = 1;
            }
            return len;
        }
        public int writeVarInt7Value(long value, boolean force_zero_write) throws IOException
        {
            int len = 0;

            if (value != 0) {
                long mask = 0x7fL;
                boolean is_negative = false;

                len = lenVarInt(value);
                if (is_negative = (value < 0)) {
                    value = -value;
                }

                // we write the first "byte" separately as it has the sign
                int b = (byte)((value >> (7*len)) & mask);
                if (is_negative)  b |= 0x40; // the sign bit
                if (len == 1)     b |= 0x80; // the terminator in case the first "byte" is the last
                write((byte)b);

                // write the rest
                switch (len - 1) {  // we already wrote 1 byte
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
                len = 0;
            }

            return len;
        }
        public int writeVarInt8Value(long value) throws IOException {
            int  len = 0;

            // figure out how many we have bytes we have to write out
            if (value != 0) {
                long mask = 0xffL;
                boolean is_negative = (value < 0);

                len = lenVarInt(value) - 1;
                if (is_negative) {
                    value = -value;
                }

                // we write the first "byte" separately as it has the sign
                int b = (byte)((value >> (8*len)) & 0x7fL);
                if (is_negative)  b |= 0x80; // the sign bit
                write((byte)b);

                // write the rest
                switch (len - 1) {  // we already wrote 1 byte
                case 6: write((byte)((value >> (8*6)) & mask));
                case 5: write((byte)((value >> (8*5)) & mask));
                case 4: write((byte)((value >> (8*4)) & mask));
                case 3: write((byte)((value >> (8*3)) & mask));
                case 2: write((byte)((value >> (8*2)) & mask));
                case 1: write((byte)((value >> (8*1)) & mask));
                case 0: write((byte)(value & mask));
                }
                len++;
            }
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
            return writeVarUInt8Value(dBits, _ib_FLOAT64_LEN);
        }
        public int writeCharValue(int c) throws IOException
        {
            // TODO: check this encoding, it is from:
            //      http://en.wikipedia.org/wiki/UTF-8
            // we probably should use some sort of Java supported
            // libaray for this.  this class might be of interest:
            //     CharsetDecoder(Charset cs, float averageCharsPerByte, float maxCharsPerByte)
            // in: java.nio.charset.CharsetDecoder

            int len = -1;

            if ((c & (~0x1FFFFF)) != 0) {
                throw new IonException("invalid character for UTF-8 output");
            }

            if ((c & (~0x7f)) == 0) {
                write((byte)(0xff & c ));
                len = 1;
            }
            else if ((c & (~0x7ff)) == 0) {
                write((byte)( 0xff & (0xC0 | (c >> 6)) ));
                write((byte)( 0xff & (0x80 | (c & 0x3F)) ));
                len = 2;
            }
            else if ((c & (~0xffff)) == 0) {
                write((byte)( 0xff & (0xE0 |  (c >> 12)) ));
                write((byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
                write((byte)( 0xff & (0x80 |  (c & 0x3F)) ));
                len = 3;
            }
            else if ((c & (~0x7ffff)) == 0) {
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
            int t = IonConstants.makeTypeDescriptor( hn.value(), len );
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

        public int writeAnnotations(String[] annotations, LocalSymbolTable symbolTable) throws IOException
        {
            int startPosition = this.position();
            int[] symbols = new int[annotations.length];

            int annotationLen = 0;
            for (int ii=0; ii<annotations.length; ii++) {
                symbols[ii] = symbolTable.findSymbol(annotations[ii]);
                annotationLen += IonBinary.lenVarUInt7(symbols[ii]);
            }

            // write the len of the list
            this.writeVarUInt7Value(annotationLen, true);

            // write the symbol id's
            for (int ii=0; ii<annotations.length; ii++) {
                symbols[ii] = symbolTable.findSymbol(annotations[ii]);
                this.writeVarUInt7Value(symbols[ii], true);
            }

            return this.position() - startPosition;
        }

        public int writeAnnotations(ArrayList<Integer> annotations)
            throws IOException
        {
            int startPosition = this.position();

            int annotationLen = 0;
            for (Integer ii : annotations) {
                annotationLen += IonBinary.lenVarUInt7(ii.intValue());
            }

            // write the len of the list
            this.writeVarUInt7Value(annotationLen, true);

            // write the symbol id's
            for (Integer ii : annotations) {
                this.writeVarUInt7Value(ii.intValue(), true);
            }

            return this.position() - startPosition;
        }


        public void writeStubStructHeader(int hn, int ln)
            throws IOException
        {
            // write the hn and ln as the typedesc, we'll patch it later.
            writeByte(IonConstants.makeTypeDescriptor(hn, ln));
        }

        /**
         * Write typedesc and length for the common case.
         */
        public int writeCommonHeader(int hn, int len)
            throws IOException
        {
            int returnlen = 0;

            // write then len in low nibble
            if (len < IonConstants.lnIsVarLen) {
                returnlen += writeByte(IonConstants.makeTypeDescriptor(hn, len));
            }
            else {
                returnlen += writeByte(IonConstants.makeTypeDescriptor(hn, IonConstants.lnIsVarLen));
                returnlen += writeVarUInt7Value(len, false);
            }
            return returnlen;
        }


        /***************************************
         *
         * Here are the write type value with type descriptor and everything
         * family of methods, these depend on the others to do much of the work
         *
         */

        public int writeSymbolWithTD(String s, LocalSymbolTable st)
            throws IOException
        {
            int sid = st.addSymbol(s);
            assert sid > 0;

            int vlen = lenVarUInt8(sid);
            int len = this.writeCommonHeader(
                                 IonConstants.tidSymbol
                                ,vlen
                           );
            len += this.writeVarUInt8Value(sid, vlen);

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
            this.writeCommonHeader(IonConstants.tidString, len);

            // now we write just the value out
            for (int ii=0; ii<s.length(); ii++) {
                char c = s.charAt(ii);
                this.writeCharValue(c);
            }

            return len;
        }

        public int writeStringData(String s) throws IOException
        {
            int len = 0;

            for (int ii=0; ii<s.length(); ii++) {
                char c = s.charAt(ii);
                len += this.writeCharValue(c);
            }

            return len;
        }

        public int writeNullWithTD(HighNibble hn) throws IOException
        {
            writeByte(hn, IonConstants.lnIsNullAtom);
            return 1;
        }
        public int writeTimestampWithTD(IonTokenReader.Type.timeinfo di)
            throws IOException
        {
            int  returnlen;

            if (di == null) {
                returnlen = this.writeCommonHeader(
                                      IonConstants.tidTimestamp
                                     ,IonConstants.lnIsNullAtom);
            }
            else {
                int vlen = IonBinary.lenIonTimestamp(di);

                returnlen = this.writeCommonHeader(
                                          IonConstants.tidTimestamp
                                         ,vlen);

                returnlen += writeTimestamp(di);
            }
            return returnlen;
        }

        public int writeTimestamp(IonTokenReader.Type.timeinfo di)
            throws IOException
        {
            int  returnlen = 0;

            if (di != null) {
                long l = di.d.getTime();
                BigDecimal bd = new BigDecimal(l);
                bd.setScale(13); // millisecond time has 13 significant digits

                int  tzoffset = (di.localOffset == null) ? 0 : di.localOffset.intValue();

                int  tzlen = IonBinary.lenVarInt7(tzoffset);
                if (tzlen == 0) tzlen = 1;

                if (di.localOffset == null) {
                    // TODO don't use magic numbers!
                    this.write((byte)(0xff & (0x80 | 0x40))); // negative 0 (no timezone)
                    returnlen ++;
                }
                else {
                    returnlen += this.writeVarInt7Value(tzoffset, true);
                }
                returnlen += this.writeDecimalContent(bd);
            }
            return returnlen;
        }


        public int writeDecimalWithTD(BigDecimal bd) throws IOException
        {
            int returnlen;
            // we only write out the '0' value as the nibble 0
            if (bd == null) {
                returnlen =
                    this.writeByte(IonDecimalImpl.NULL_DECIMAL_TYPEDESC);
            }
            else if (BigDecimal.ZERO.equals(bd)) {
                returnlen =
                    this.writeByte(IonDecimalImpl.ZERO_DECIMAL_TYPEDESC);
            }
            else {
                // otherwise we to it the hard way ....
                int len = IonBinary.lenIonDecimal(bd);

                // FIXME: this assumes len < 14!
                assert len < IonConstants.lnIsVarLen;
                returnlen = this.writeByte(
                        IonConstants.makeTypeDescriptor(
                            IonConstants.tidDecimal
                          , len
                        )
                      );
                int wroteDecimalLen = writeDecimalContent(bd);
                assert wroteDecimalLen == len;
                returnlen += wroteDecimalLen;
            }
            return returnlen;
        }

        // also used by writeDate()
        public int writeDecimalContent(BigDecimal bd) throws IOException
        {
            int returnlen = 0;
            // we only write out the '0' value as the nibble 0
            if (bd != null && !BigDecimal.ZERO.equals(bd)) {
                // otherwise we do it the hard way ....
                BigInteger mantissa = bd.unscaledValue();

                boolean isNegative = (mantissa.compareTo(BigInteger.ZERO) < 0);
                if (isNegative) {
                    mantissa = mantissa.negate();
                }

                byte[] bits  = mantissa.toByteArray();
                int    scale = bd.scale();

                // Ion stores exponent, BigDecimal uses the negation "scale"
                int exponent = -scale;
                returnlen += this.writeVarInt7Value(exponent, true);

                // If the first bit is set, we can't use it for the sign,
                // and we need to write an extra byte to hold it.
                boolean needExtraByteForSign = ((bits[0] & 0x80) != 0);
                if (needExtraByteForSign)
                {
                    this.write(isNegative ? 0x80 : 0x00);
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
