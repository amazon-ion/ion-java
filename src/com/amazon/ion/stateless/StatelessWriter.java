package com.amazon.ion.stateless;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import com.amazon.ion.IonException;
import com.amazon.ion.impl.IonTokenReader;

final class StatelessWriter {

    static void writeAnnotations(int[] annotations, int valueAndHeaderLength, ByteArrayOutputStream out) throws IOException {
        int annotationsLength = lenAnnotationList(annotations);
        int totalLength = valueAndHeaderLength+annotationsLength+IonBinary.lenVarUInt7(annotationsLength);
        writeCommonHeader(IonConstants.tidTypedecl, totalLength, out);
        StatelessWriter.writeVarUInt7Value(annotationsLength, true, out);
        for (int i : annotations) {
            StatelessWriter.writeVarUInt7Value(i, true, out);
        }
    }

    private static int lenAnnotationList(int[] annotations)
    {
        int annotationLen = 0;
        // add up the length of the encoded symbols
        for (Integer ii : annotations) {
            int symid = ii.intValue();
            annotationLen += IonBinary.lenVarUInt7(symid);
        }
        return annotationLen;
    }

    static final void writeTypeDescriptor(int highNibble, int lowNibble, ByteArrayOutputStream out) {
        assert highNibble == (highNibble & 0xF);
        assert lowNibble == (lowNibble & 0xF);
        out.write((highNibble << 4) | lowNibble);
    }


    static public int writeVarUInt7Value(int value, boolean force_zero_write,ByteArrayOutputStream out) throws IOException
    {
        int mask = 0x7F;
        int len = IonBinary.lenVarUInt7(value);

        switch (len - 1) {
        case 4: out.write((byte)((value >> (7*4)) & mask));
        case 3: out.write((byte)((value >> (7*3)) & mask));
        case 2: out.write((byte)((value >> (7*2)) & mask));
        case 1: out.write((byte)((value >> (7*1)) & mask));
        case 0: out.write((byte)((value & mask) | 0x80L));
                break;
        case -1: // or 0
            if (force_zero_write) {
                out.write((byte)0x80);
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
    static public int writeVarUInt8Value(long value, int len,ByteArrayOutputStream out) throws IOException
    {
        long mask = 0xffL;

        switch (len) {
        case 8: out.write((byte)((value >> (56)) & mask));
        case 7: out.write((byte)((value >> (48)) & mask));
        case 6: out.write((byte)((value >> (40)) & mask));
        case 5: out.write((byte)((value >> (32)) & mask));
        case 4: out.write((byte)((value >> (24)) & mask));
        case 3: out.write((byte)((value >> (16)) & mask));
        case 2: out.write((byte)((value >> (8)) & mask));
        case 1: out.write((byte)(value & mask));
                break;
        }
        return len;
    }

    static public int writeVarInt7Value(int value, boolean force_zero_write,ByteArrayOutputStream out) throws IOException
    {
        int len = 0;

        if (value != 0) {
            int mask = 0x7F;
            boolean is_negative = false;

            len = IonBinary.lenVarInt7(value);
            if (is_negative = (value < 0)) {
                value = -value;
            }

            // we write the first "byte" separately as it has the sign
            int b = (byte)((value >> (7*(len-1))) & mask);
            if (is_negative)  b |= 0x40; // the sign bit only on the first byte
            if (len == 1)     b |= 0x80; // the terminator in case the first "byte" is the last
            out.write((byte)b);

            // write the rest
            switch (len) {  // we already wrote 1 byte
            case 5: out.write((byte)((value >> (7*3)) & mask));
            case 4: out.write((byte)((value >> (7*2)) & mask));
            case 3: out.write((byte)((value >> (7*1)) & mask));
            case 2: out.write((byte)((value & mask) | 0x80));
            case 1: // do nothing
            }
        }
        else if (force_zero_write) {
            out.write((byte)0x80);
            len = 1;
        }
        return len;
    }

    static public int writeFloatValue(double d,ByteArrayOutputStream out) throws IOException
    {
        if (Double.valueOf(d).equals(IonBinary.DOUBLE_POS_ZERO))
        {
            // pos zero special case
            return 0;
        }
        long dBits = Double.doubleToRawLongBits(d);
        return writeVarUInt8Value(dBits, IonBinary._ib_FLOAT64_LEN,out);
    }

    static public void writeCharValue(int c,ByteArrayOutputStream out) throws IOException
    {
        // TODO: check this encoding, it is from:
        //      http://en.wikipedia.org/wiki/UTF-8
        // we probably should use some sort of Java supported
        // libaray for this.  this class might be of interest:
        //     CharsetDecoder(Charset cs, float averageCharsPerByte, float maxCharsPerByte)
        // in: java.nio.charset.CharsetDecoder

        if ((c & (~0x1FFFFF)) != 0) {
            throw new IonException("invalid character for UTF-8 output");
        }

        if ((c & (~0x7f)) == 0) {
            out.write(c);
        }
        else if ((c & (~0x7ff)) == 0) {
            out.write((0xC0 | (c >> 6)) );
            out.write((0x80 | (c & 0x3F)) );
        }
        else if ((c & (~0xffff)) == 0) {
            out.write((0xE0 |  (c >> 12)) );
            out.write((0x80 | ((c >> 6) & 0x3F)) );
            out.write((0x80 |  (c & 0x3F)) );
        }
        else if ((c & (~0x7ffff)) == 0) {
            out.write((0xF0 |  (c >> 18)) );
            out.write((0x80 | ((c >> 12) & 0x3F)) );
            out.write((0x80 | ((c >> 6) & 0x3F) ));
            out.write((0x80 | (c & 0x3F)) );
        }
        else {
            throwUTF8Exception(out.size());
        }
    }

    /**
     * Write typedesc and length for the common case.
     */
    static public void writeCommonHeader(int hn, int len,ByteArrayOutputStream out)
        throws IOException
    {
        // write then len in low nibble
        if (len < IonConstants.lnIsVarLen) {
            out.write(IonConstants.makeTypeDescriptor(hn, len));
        }
        else {
            out.write(IonConstants.makeTypeDescriptor(hn, IonConstants.lnIsVarLen));
            writeVarUInt7Value(len, false,out);
        }
    }

    static final Charset _encoding = Charset.forName("UTF-8");
    static final CharsetEncoder _encoder = _encoding.newEncoder();

    static public ByteBuffer writeStringData(String s) throws IOException
    {
        ByteBuffer result = ByteBuffer.allocate(4*s.length());
        int c;
        for (int i=0; i<s.length(); i++) {
            c = s.charAt(i);
            if ((c & (~0x1FFFFF)) != 0) {
                throw new IonException("invalid character for UTF-8 output");
            }

            if ((c & (~0x7f)) == 0) {
                result.put((byte) c);
            }
            else if ((c & (~0x7ff)) == 0) {
                result.put((byte) (0xC0 | (c >> 6)) );
                result.put((byte) (0x80 | (c & 0x3F)) );
            }
            else if ((c & (~0xffff)) == 0) {
                result.put((byte) (0xE0 |  (c >> 12)) );
                result.put((byte) (0x80 | ((c >> 6) & 0x3F)) );
                result.put((byte) (0x80 |  (c & 0x3F)) );
            }
            else if ((c & (~0x7ffff)) == 0) {
                result.put((byte) (0xF0 |  (c >> 18)) );
                result.put((byte) (0x80 | ((c >> 12) & 0x3F)) );
                result.put((byte) (0x80 | ((c >> 6) & 0x3F) ));
                result.put((byte) (0x80 | (c & 0x3F)) );
            }
            else {
                throwUTF8Exception(result.position());
            }
        }
        result.limit(result.position()).rewind();
        return result;
    }

    static public int writeTimestamp(IonTokenReader.Type.timeinfo di,ByteArrayOutputStream out)
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
                out.write((byte)(0xff & (0x80 | 0x40))); // negative 0 (no timezone)
                returnlen ++;
            }
            else {
                returnlen += writeVarInt7Value(tzoffset, true,out);
            }
            returnlen += writeDecimalContent(bd,out);
        }
        return returnlen;
    }

    // also used by writeDate()
    static public int writeDecimalContent(BigDecimal bd,ByteArrayOutputStream out) throws IOException
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
            returnlen += writeVarInt7Value(exponent, true,out);

            // If the first bit is set, we can't use it for the sign,
            // and we need to write an extra byte to hold it.
            boolean needExtraByteForSign = ((bits[0] & 0x80) != 0);
            if (needExtraByteForSign)
            {
                out.write(isNegative ? 0x80 : 0x00);
                returnlen++;
            }
            else if (isNegative)
            {
                bits[0] |= 0x80;
            }
            out.write(bits, 0, bits.length);
            returnlen += bits.length;
        }
        return returnlen;
    }


    static void throwUTF8Exception(int pos)
    {
        throw new IonException("Invalid UTF-8 character encounter in a string at pos " + pos);
    }

}
