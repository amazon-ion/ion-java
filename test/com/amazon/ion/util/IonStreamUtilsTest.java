// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static com.amazon.ion.impl.PrivateIonConstants.BINARY_VERSION_MARKER_1_0;
import static com.amazon.ion.impl.PrivateUtils.EMPTY_BYTE_ARRAY;
import static com.amazon.ion.util.IonStreamUtils.isIonBinary;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class IonStreamUtilsTest
{

    public static final byte[] SHORT_BUFFER_1 = { (byte) 0xE0 };
    public static final byte[] SHORT_BUFFER_2 = { (byte) 0xE0,
                                                  (byte) 0x01 };
    public static final byte[] SHORT_BUFFER_3 = { (byte) 0xE0,
                                                  (byte) 0x01,
                                                  (byte) 0x00 };

    public static final byte[] MID_BUFFER = { (byte) 0xAB,
                                              (byte) 0xCD,
                                              (byte) 0xE0,
                                              (byte) 0x01,
                                              (byte) 0x00,
                                              (byte) 0xEA,
                                              (byte) 0x0F };

    @Test
    public void testIsIonBinaryGood()
    {
        assertTrue(isIonBinary(BINARY_VERSION_MARKER_1_0));
        assertTrue(isIonBinary(BINARY_VERSION_MARKER_1_0, 0, 4));
        assertTrue(isIonBinary(MID_BUFFER, 2, 5));
    }

    @Test
    public void testIsIonBinaryNullBuffer()
    {
        assertEquals(false, isIonBinary(null));
        assertEquals(false, isIonBinary(null, 1, 20));
    }

    @Test
    public void testIsIonBinaryShortBufer()
    {
        assertEquals(false, isIonBinary(EMPTY_BYTE_ARRAY));
        assertEquals(false, isIonBinary(SHORT_BUFFER_1));
        assertEquals(false, isIonBinary(SHORT_BUFFER_2));
        assertEquals(false, isIonBinary(SHORT_BUFFER_3));

        assertEquals(false, isIonBinary(BINARY_VERSION_MARKER_1_0, 0, 0));
        assertEquals(false, isIonBinary(BINARY_VERSION_MARKER_1_0, 0, 1));
        assertEquals(false, isIonBinary(BINARY_VERSION_MARKER_1_0, 0, 2));
        assertEquals(false, isIonBinary(BINARY_VERSION_MARKER_1_0, 0, 3));
    }
}
