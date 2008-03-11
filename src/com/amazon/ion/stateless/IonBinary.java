/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.stateless;

import static com.amazon.ion.stateless.IonConstants.BINARY_VERSION_MARKER_1_0;
import static com.amazon.ion.stateless.IonConstants.BINARY_VERSION_MARKER_SIZE;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.amazon.ion.impl.IonTokenReader;


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

    static final Double DOUBLE_POS_ZERO = Double.valueOf(0.0);

    private IonBinary() { }

    public static boolean startsWithBinaryVersionMarker(byte[] b,int offset,int length)
    {
        if (offset+length > b.length) throw new IllegalArgumentException("Invalid length given.");
        if (length < BINARY_VERSION_MARKER_SIZE) return false;

        for (int i = 0; i < BINARY_VERSION_MARKER_SIZE; i++)
        {
            if (BINARY_VERSION_MARKER_1_0[i] != b[i+offset]) return false;
        }
        return true;
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

    /**
     * Variable-length, high-bit-terminating integer, 7 data bits per byte.
     */
    public static int lenVarUInt7(int intVal) {  // we write a lot of these
        int len = 0;

        if (intVal != 0) {
            if (intVal < 0) {
                return _ib_VAR_INT32_LEN_MAX;
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

    public static int lenVarUInt8(long longVal) {
        int len = 0;

        // figure out how many we have bytes we have to write out
        if (longVal != 0) {
            if (longVal < 0) {
                return _ib_INT64_LEN_MAX;
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

    public static int lenIonFloat(double value) {
        if (Double.valueOf(value).equals(DOUBLE_POS_ZERO))
        {
            // pos zero special case
            return 0;
        }

        // always 8-bytes for IEEE-754 64-bit
        return _ib_FLOAT64_LEN;
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
}
