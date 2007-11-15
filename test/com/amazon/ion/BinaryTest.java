/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Arrays;

import com.amazon.ion.system.StandardIonSystem;

public class BinaryTest extends IonTestCase
{
    private static final IonSystem sys = new StandardIonSystem();

    private static byte[] hexToBytes(final String hex)
    {
        String[] hexChunks = hex.split("\\s+");
        byte[] data = new byte[hexChunks.length];
        for (int i = 0; i < hexChunks.length; i++)
        {
            final String hexChunk = hexChunks[i];
            final int ordinal = Integer.parseInt(hexChunk, 16);
            if ((ordinal & 0xFFFFFF00) != 0)
            {
                throw new IllegalArgumentException("Bad Chunk: " + hexChunk);
            }
            // this is safe because we only have the low bytes set
            data[i] = (byte) ordinal;
        }
        return data;
    } 

    private static String bytesToHex(final byte[] bytes)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++)
        {
            byte octet = bytes[i];
            int ordinal = octet & 0xFF;
            builder.append(String.format("%02X", ordinal));
            if ((i + 1) < bytes.length)
            {
                builder.append(' ');
            }
        }
        return builder.toString();
    }

    private static String MAGIC_COOKIE = "10 14 01 00 ";

    // MC + $ion_1_0::{symbols : null.struct}
    private static String EMPTY_HEADER = MAGIC_COOKIE + "E5 81 82 D2 87 DF ";

    /**
     * Loads a string literal as Ion bytes no cookie required.
     * We encode the string literal as hex digits as it is convenient to read.
     *
     * @param   hex     A simple space delimited hex encoding.
     */
    private static IonValue ion(final String hex)
    {
        return sys.singleValue(hexToBytes(MAGIC_COOKIE + hex));
    }

    /** Converts a single value to bytes using an empty datagram */
    private static byte[] ionBytes(IonValue val)
    {
        IonDatagram dg = sys.newDatagram(val);
        return dg.toBytes();
    }

    /** Dumps byte arrays to hex strings */
    private static String dump(byte[]... packets)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < packets.length; i++)
        {
            byte[] packet = packets[i];
            builder.append('"');
            builder.append(bytesToHex(packet));
            builder.append('"');

            if ((i + 1) < packets.length)
            {
                builder.append('\n');
            }
        }
        return builder.toString();
    }

    private static void assertDoubleBits(IonValue val, long bits)
    {
        assertTrue(val instanceof IonFloat);
        IonFloat fval = (IonFloat) val;
        long test = Double.doubleToRawLongBits(fval.doubleValue());
        assertTrue(String.format("%X\n%X", test, bits), test == bits);
    }

    public void testBinReadNull01()
    {
        // null.null
        IonValue val = ion("0F");

        assertTrue(val instanceof IonNull);
        assertTrue(val.isNullValue());
    }
    
    public void testBinReadNull02()
    {
        // name::null.null
        IonValue val = ion("E3 81 84 0F");

        assertTrue(val instanceof IonNull);
        assertTrue(val.isNullValue());
    }

    public void testBinReadFloat01()
    {
        // 1e0
        IonValue val = ion("48 3F F0 00 00 00 00 00 00");
        
        assertTrue(val instanceof IonFloat);
        assertTrue(((IonFloat) val).doubleValue() == 1.0);
    }

    public void testBinReadFloat02()
    {
        // $ion_1_0::{} 1e0
        IonValue val = ion("E3 81 82 D0 48 3F F0 00 00 00 00 00 00");
        
        assertTrue(val instanceof IonFloat);
        assertTrue(((IonFloat) val).doubleValue() == 1.0);
    }

    public void testBinReadFloat03()
    {
        // approx 1.79769313486231e300
        IonValue val = ion("48 7E 45 79 8E E2 30 8C 26");
        assertDoubleBits(val, 0x7E45798EE2308C26L);
    }

    public void testBinReadFloat04()
    {
        // approx -1.2278379192877e-276
        IonValue val = ion("48 86 A5 C3 F2 8D 5E C5 4A");
        assertDoubleBits(val, 0x86A5C3F28D5EC54AL);
    }

    public void testBinWriteFloat01()
    {
        IonFloat fval = sys.newNullFloat();
        fval.setValue(-1.0);
        byte[] raw = ionBytes(fval);
        byte[] ref = hexToBytes(EMPTY_HEADER + "48 BF F0 00 00 00 00 00 00");
        
        assertTrue(dump(raw, ref), Arrays.equals(raw, ref));
    }

    public void testBinWriteFloat02()
    {
        IonFloat fval = sys.newNullFloat();
        // approx 1.79769313486231e300
        fval.setValue(Double.longBitsToDouble(0x7E45798EE2308C26L));
        byte[] raw = ionBytes(fval);
        byte[] ref = hexToBytes(EMPTY_HEADER + "48 7E 45 79 8E E2 30 8C 26");
        
        assertTrue(dump(raw, ref), Arrays.equals(raw, ref));
    }

    public void testBinWriteFloat03()
    {
        IonFloat fval = sys.newNullFloat();
        // approx -1.2278379192877e-276
        fval.setValue(Double.longBitsToDouble(0x86A5C3F28D5EC54AL));
        byte[] raw = ionBytes(fval);
        byte[] ref = hexToBytes(EMPTY_HEADER + "48 86 A5 C3 F2 8D 5E C5 4A");
        
        assertTrue(dump(raw, ref), Arrays.equals(raw, ref));
    }

}

