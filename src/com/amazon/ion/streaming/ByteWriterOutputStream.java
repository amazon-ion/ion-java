/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

/*
 *   No longer needed 
 * 
 * 
 
import com.amazon.ion.IonException;
import com.amazon.ion.impl.IonBinary;
import com.amazon.ion.impl.IonTokenReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;


public final class ByteWriterOutputStream
    implements ByteWriter
{
    private static final int _ib_FLOAT64_LEN         =    8;
    private static final Double DOUBLE_POS_ZERO = Double.valueOf(0.0);

    SimpleByteBuffer.SimpleByteWriter _out;
    
    public ByteWriterOutputStream(SimpleByteBuffer.SimpleByteWriter out) {
        _out = out;
    }

    public void insert(int length)
    {
        throw new UnsupportedOperationException();
    }

    public int position()
    {
        throw new UnsupportedOperationException();
    }

    public void position(int newPosition)
    {
        throw new UnsupportedOperationException();
    }

    public void remove(int length)
    {
        throw new UnsupportedOperationException();
    }

    public void write(byte b) throws IOException
    {
        _out.write(b);
    }

    public void write(byte[] dst, int start, int len) throws IOException
    {
        _out.write(dst, start, len);
    }

    public void writeTypeDesc(int typeDescByte) throws IOException
    {
        write((byte)(typeDescByte & 0xff));
    }

    public int writeTypeDescWithLength(int typeid, int valueLength) throws IOException
    {
        int written_length = 1;
        int td = ((typeid & 0xf) << 4);
        if (valueLength >= 14) {
            td |= (byte)14;
            writeTypeDesc((byte)td);
            written_length += writeVarUInt(valueLength, true);
        }
        else {
            td |= (valueLength & 0xf);
            writeTypeDesc((byte)td);
        }
        return written_length;
    }

    public int writeInt(int value, boolean force_zero_write) throws IOException
    {
        // THIS IS a buggy copy, it should be writing 8 bits per byte, not 7!
        
        throw new UnsupportedOperationException("E_NOT_IMPL");
       
    }

    public int writeInt(int value, int lenToWrite)
    {
        // TODO ??? do we need this?
        throw new UnsupportedOperationException();
    }

    public int writeVarInt(int value, boolean force_zero_write) throws IOException
    {

        int len = 0;

        if (value != 0) {
            int mask = 0x7F;
            boolean is_negative = false;

            len = IonBinary.lenVarInt7(value);
            if (is_negative = (value < 0)) {
                int v2 = -value;
                if (v2 == value) throw new IonException("value out of bounds for int (too small)");
                value = v2;
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
    
    public int writeVarUInt(int value, boolean force_zero_write) throws IOException
    {
        int mask = 0x7F;
        int len = IonBinary.lenVarUInt7(value);

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

    public int writeVarUInt(int value, int fixed_size) throws IOException
    {
        int mask = 0x7F;
        int len = IonBinary.lenVarUInt7(value);
    
        if (fixed_size > 0) {
            if (fixed_size < len) {
                throwException(
                         "overflow, fixed size ("
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
        return len
    }

    public int writeLong(long value, boolean force_zero_write) throws IOException
    {
        int  len = 0;

        // figure out how many we have bytes we have to write out
        if (value != 0) {
            long mask = 0xffL;
            boolean is_negative = (value < 0);

            len = IonBinary.lenVarInt(value) - 1;
            if (is_negative) {
                long v2 = -value;
                if (v2 == value) throw new IonException("value out of bounds for long (too small)");
                value = v2;
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
        else if (force_zero_write) {
            write((byte)0x80);
            len = 1;
        }
        return len;
    }

    public int writeVarLong(long value, boolean force_zero_write) throws IOException
    {
        int len = 0;

        if (value != 0) {
            long mask = 0x7fL;
            boolean is_negative = false;

            len = IonBinary.lenVarInt(value);
            if (is_negative = (value < 0)) {
                long v2 = -value;
                if (v2 == value) throw new IonException("value out of bounds for long (too small)");
                value = v2;
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

    public int writeVarULong(long value, boolean force_zero_write) throws IOException
    {
        int mask = 0x7F;
        int len = IonBinary.lenVarUInt7(value);

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
        case -1: // or 0
            if (force_zero_write) {
                write((byte)0x80);
                len = 1;
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
            returnlen += writeInt(exponent, true);

            // If the first bit is set, we can't use it for the sign,
            // and we need to write an extra byte to hold it.
            boolean needExtraByteForSign = ((bits[0] & 0x80) != 0);
            if (needExtraByteForSign)
            {
                write((byte)(isNegative ? 0x80 : 0x00));
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
                write((byte)(0xff & (0x80 | 0x40))); // negative 0 (no timezone)
                returnlen ++;
            }
            else {
                returnlen += writeInt(tzoffset, true);
            }
            returnlen += writeDecimal(bd);
        }
        return returnlen;
    }
    
    static final int high_surrogate_value = 0xD800;
    static final int low_surrogate_value = 0xDC00;
    static final int surrogate_mask = 0xFC00; // 0x3f << 10; or the top 6 bits
    static final int surrogate_utf32_offset = 0x10000;
    // TODO: port this surrogate handling back into the various writers (and 
    //       stringlen routines) perhaps while refactoring these write 
    //       routines into a common class/interface/etc
    // done for IonBinary lenString and lenChar
    public int writeString(String value) throws IOException
    {
        int len = 0;

        for (int ii=0; ii<value.length(); ii++) {
            int c = value.charAt(ii);
            if (c < 128) {
                // check out the common case and don't waste time on it.
                _out.write((byte)(c & 0xff));
                len++;
            }
            else {
                int surrogate_test = (c & surrogate_mask);
                if (surrogate_test != 0) {
                    if (surrogate_test == high_surrogate_value) {
                        ii++;
                        if (ii >= value.length()) {
                            throwException("high surrogate character missing low surrogate, endcountered in Java string, invalid UTF-16 value");
                        }
                        int c2 = value.charAt(ii);
                        if ((c2 & surrogate_mask) != low_surrogate_value) {
                            throwException("unmatched high surrogate character endcountered in Java string, invalid UTF-16 value");
                        }
                        c = ((c & ~surrogate_mask) << 10) | (c2 & ~surrogate_mask);
                        c +=  surrogate_utf32_offset;
                    }
                    else if (surrogate_test == low_surrogate_value) {
                        throwException("unmatched low surrogate character endcountered in Java string (no preceding high surrogate), invalid UTF-16 value");
                    }
                    len += writeChar(c);
                }
            }
        }
        return len;
    }
    
    final public int writeChar(int c) throws IOException
    {
        // TODO: check this encoding, it is from:
        //      http://en.wikipedia.org/wiki/UTF-8
        if ((c & (~0x1FFFFF)) != 0) {
            throwException("invalid character for UTF-8 output");
        }

        if ((c & (~0x7f)) == 0) {
            write((byte)(0xff & c ));
            return 1;
        }
        else if ((c & (~0x7ff)) == 0) {
            write((byte)( 0xff & (0xC0 | (c >> 6)) ));
            write((byte)( 0xff & (0x80 | (c & 0x3F)) ));
            return 2;
        }
        else if ((c & (~0xffff)) == 0) {
            write((byte)( 0xff & (0xE0 |  (c >> 12)) ));
            write((byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
            write((byte)( 0xff & (0x80 |  (c & 0x3F)) ));
            return 3;
        }
        else if ((c & (~0x7ffff)) == 0) {
            write((byte)( 0xff & (0xF0 |  (c >> 18)) ));
            write((byte)( 0xff & (0x80 | ((c >> 12) & 0x3F)) ));
            write((byte)( 0xff & (0x80 | ((c >> 6) & 0x3F)) ));
            write((byte)( 0xff & (0x80 | (c & 0x3F)) ));
            return 4;
        }

        throwUTF8Exception();
        return -1; // Eclipse can't figure out that the call above never returns
    }
    void throwUTF8Exception() throws IOException 
    {
        throwException("Invalid character encountered writing a UTF-32 character");
    }
    void throwException(String msg) throws IOException {
        throw new IOException(msg);
    }

}
*/