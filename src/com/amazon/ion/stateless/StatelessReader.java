/**
 *
 */
package com.amazon.ion.stateless;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.impl.BlockedBuffer;
import com.amazon.ion.impl.IonConstants;
import com.amazon.ion.impl.IonTokenReader;

public final class StatelessReader
    {
        /**
         * Read exactly one byte of input.
         *
         * @return 0x00 through 0xFF as a positive int.
         * @throws IOException if there's other problems reading input.
         */
        static public int readToken(ByteBuffer in) throws IOException
        {
            return in.get() & 0x00FF;
        }

        static IonType get_iontype_from_tid(int tid)
        {
            IonType t = null;
            switch (tid) {
            case IonConstants.tidNull:      // 0
                t = IonType.NULL;
                break;
            case IonConstants.tidBoolean:   // 1
                t = IonType.BOOL;
                break;
            case IonConstants.tidPosInt:    // 2
            case IonConstants.tidNegInt:    // 3
                t = IonType.INT;
                break;
            case IonConstants.tidFloat:     // 4
                t = IonType.FLOAT;
                break;
            case IonConstants.tidDecimal:   // 5
                t = IonType.DECIMAL;
                break;
            case IonConstants.tidTimestamp: // 6
                t = IonType.TIMESTAMP;
                break;
            case IonConstants.tidSymbol:    // 7
                t = IonType.SYMBOL;
                break;
            case IonConstants.tidString:    // 8
                t = IonType.STRING;
                break;
            case IonConstants.tidClob:      // 9
                t = IonType.CLOB;
                break;
            case IonConstants.tidBlob:      // 10 A
                t = IonType.BLOB;
                break;
            case IonConstants.tidList:      // 11 B
                t = IonType.LIST;
                break;
            case IonConstants.tidSexp:      // 12 C
                t = IonType.SEXP;
                break;
            case IonConstants.tidStruct:    // 13 D
                t = IonType.STRUCT;
                break;
            case IonConstants.tidTypedecl:  // 14 E
            default:
                throw new IonException("unrecognized value type encountered: "+tid);
            }
            return t;
        }

        static public IonType readActualTypeDesc(ByteBuffer in) throws IOException
        {
            int c = readToken(in);
            int typeid = IonConstants.getTypeCode(c);
            if (typeid == IonConstants.tidTypedecl) {
                readLength(in,typeid, IonConstants.getLowNibble(c));
                int alen = readVarUInt7IntValue(in);
                in.position(in.position()+alen);
                c = readToken(in);
                typeid = IonConstants.getTypeCode(c);
            }
            in.position(in.position()-1); //Backup so as not to swallow length.
            return get_iontype_from_tid(typeid);
        }

        /**
         * @return The length of the element skipped.
         * @throws IOException
         */
        static public int readLength(ByteBuffer in, int expectedType) throws IOException {
            int c = readToken(in);
            int typeid = IonConstants.getTypeCode(c);
            if (expectedType != -1 && typeid != expectedType)
                throw new IonException("Encountered unexpected type. Expecting: "+expectedType+", got: "+typeid);
            return readLength(in,typeid, IonConstants.getLowNibble(c));
        }

        /** Skip over the current element.
         * @throws IOException
         */
        static public void skip(ByteBuffer in) throws IOException {
            int length = readLength(in,-1);
            in.position(in.position()+length);
        }

        static public int getNumElementsInList(int length, ByteBuffer in) throws IOException {
            int result = 0;
            int start = in.position();
            int end = start + length;
            assert end >= in.limit();
            while (in.position() < end) {
                skip(in);
                result++;
            }
            in.position(start);
            return result;
        }

        static public int[] readAnnotations(ByteBuffer in) throws IOException
        {
            int c = readToken(in);
            int typeid = IonConstants.getTypeCode(c);
            if (typeid != IonConstants.tidTypedecl) {
                in.position(in.position()-1);
                return null;
            }

            readLength(in,typeid, IonConstants.getLowNibble(c));
            ArrayList<Integer> annotations = new ArrayList<Integer>();

            int annotationLen = readVarUInt7IntValue(in);
            int annotationPos = in.position(); // pos at the first ann sid
            int annotationEnd = annotationPos + annotationLen;

            while(in.position() < annotationEnd) {
                annotations.add(readVarUInt7IntValue(in));
            }

            int[] result = new int[annotations.size()];
            for (int i=0;i<result.length;i++) {
                result[i] = annotations.get(i);
            }
            return result;
        }

        static public int readLength(ByteBuffer in,int td, int ln) throws IOException
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
                    return readVarUInt7IntValue(in);
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
                    return readVarUInt7IntValue(in);
                default:
                    return ln;
                }
            case IonConstants.tidUnused: // unused(15)
            default:
                // TODO use InvalidBinaryDataException
                throw new BlockedBuffer.BlockedBufferException("invalid type id encountered: " + td);
            }
        }

        static public long readVarUInt8LongValue(ByteBuffer in,int len) throws IOException {
            long retvalue = 0;
            int b;
            if (len > 8)
                throw new IonException("overflow attempt to read long value into an int");
            for (int i = 0;i<len;i++) {
                b = readToken(in);
                retvalue = (retvalue << 8) | b;
            }
            return retvalue;
        }

        static public int readVarUInt8IntValue(ByteBuffer in,int len) throws IOException {
            int retvalue = 0;
            int b;
            if (len > 4)
                throw new IonException("overflow attempt to read long value into an int");
            for (int i = 0;i<len;i++) {
                b = readToken(in);
                retvalue = (retvalue << 8) | b;
            }
            return retvalue;
        }

        static public int readVarUInt7IntValue(ByteBuffer in) throws IOException {
            int retvalue = 0;
            int b;
            do {
                b = in.get();
                retvalue = (retvalue << 7) | (b & 0x7F);
            } while (b >= 0);

            return retvalue;
        }

        static public int readVarInt7IntValue(ByteBuffer in) throws IOException
        {
            int     retvalue = 0;
            int     b = readToken(in);
            // read the first byte - it has the sign bit
            boolean is_negative = (b & 0x40) != 0;
            retvalue = (b & 0x3F);

            while ((b & 0x80) == 0) {
                b = readToken(in);
                retvalue = (retvalue << 7) | (b & 0x7F);
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
        static public Integer readVarInt7WithNegativeZero(ByteBuffer in) throws IOException
        {
            int     retvalue = 0;
            boolean is_negative = false;
            int     b;

            // sythetic label "done" (yuck)
done:       for (;;) {
                // read the first byte - it has the sign bit
                b = readToken(in);
                if ((b & 0x40) != 0) {
                    is_negative = true;
                }
                retvalue = (b & 0x3F);
                if ((b & 0x80) != 0) break done;

                // for the second byte we shift our eariler bits just as much,
                // but there are fewer of them there to shift
                b = readToken(in);
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                b = readToken(in);
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                b = readToken(in);
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // for the rest, they're all the same
                b = readToken(in);
                retvalue = (retvalue << 7) | (b & 0x7F);
                if ((b & 0x80) != 0) break done;

                // if we get here we have more bits than we have room for :(
                throw new IonException("var int overflow at: "+in.position());
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

        static public double readFloatValue(ByteBuffer in,int len) throws IOException
        {
            if (len == 0)
            {
                return 0d;
            }

            if (len != 8)
            {
                throw new IonException("Length of float read must be 0 or 8");
            }

            long dBits = readVarUInt8LongValue(in,len);
            return Double.longBitsToDouble(dBits);
        }

        static public BigDecimal readDecimalValue(ByteBuffer in,int len) throws IOException
        {
            BigDecimal bd;

            // we only write out the '0' value as the nibble 0
            if (len == 0) {
                bd = new BigDecimal(0, MathContext.DECIMAL128);
            }
            else {
                // otherwise we to it the hard way ....
                int         startpos = in.position();
                int         exponent = readVarInt7IntValue(in);
                int         bitlen = len - (in.position() - startpos);

                BigInteger value;
                if (bitlen > 0)
                {
                    byte[] bits = new byte[bitlen];
                    in.get(bits, 0, bitlen);

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

        static public IonTokenReader.Type.timeinfo readTimestampValue(ByteBuffer in,int len) throws IOException
        {
            if (len < 1) return null;
            int startpos = in.position();

            // first the timezone, and -0 is unknown
            Integer tz = readVarInt7WithNegativeZero(in);

            // now the time part
            BigDecimal bd = readDecimalValue(in,len - (in.position() - startpos));

            // now we put it together
            IonTokenReader.Type.timeinfo ti = new IonTokenReader.Type.timeinfo();
            ti.d = new Date(bd.longValue());
            ti.localOffset = tz;

            return ti;
        }

//        static public String readString(ByteBuffer in,int len) throws IOException
//        {
//            StringBuffer sb = new StringBuffer(len);
//            int c;
//            int endPosition = in.position() + len;
//
//            while (in.position() < endPosition) {
//                c = readChar(in);
//                sb.append((char)c);
//            }
//            return sb.toString();
//        }
//
//        static public int readChar(ByteBuffer in) throws IOException {
//            int c = -1, b;
//
//            b = readToken(in);
//            if (b < 0) return -1;
//            if ((b & 0x80) == 0) {
//                return b;
//            }
//
//            if ((b & 0xe0) == 0xc0) {
//                c = (b & ~0xe0);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//            }
//            else if ((b & 0xf0) == 0xe0) {
//                c = (b & ~0xf0);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//            }
//            else if ((b & 0xf8) == 0xf0) {
//                c = (b & ~0xf8);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//                b = readToken(in);
//                if ((b & 0xc0) != 0x80) throwUTF8Exception(in.position());
//                c <<= 6;
//                c |= (b & ~0x80);
//            }
//            else {
//                throwUTF8Exception(in.position());
//            }
//            return c;
//        }
//
//        static void throwUTF8Exception(int pos)
//        {
//            throw new IonException("Invalid UTF-8 character encounter in a string at pos " + pos);
//        }
    }