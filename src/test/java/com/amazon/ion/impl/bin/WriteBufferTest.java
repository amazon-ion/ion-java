// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.bin;

import static com.amazon.ion.TestUtils.hexDump;
import static com.amazon.ion.impl.bin.WriteBuffer.varUIntLength;
import static com.amazon.ion.impl.bin.WriteBuffer.writeVarUIntTo;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazon.ion.impl.bin.utf8.Utf8StringEncoder;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class WriteBufferTest
{
    // XXX make this a prime to make it more likely that we collide on the edges of the buffer
    private static BlockAllocator ALLOCATOR = BlockAllocatorProviders.basicProvider().vendAllocator(11);
    private ByteArrayOutputStream out;
    private WriteBuffer buf;
    private AtomicBoolean endOfBufferReached = new AtomicBoolean(false);

    @BeforeEach
    public void setup() throws IOException
    {
        buf = new WriteBuffer(ALLOCATOR, () -> endOfBufferReached.set(true));
        out = new ByteArrayOutputStream();
    }

    @AfterEach
    public void teardown()
    {
        buf = null;
        out.reset();
    }

    private byte[] bytes()
    {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try
        {
            buf.writeTo(out);
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    private void assertBuffer(final byte[] expected)
    {

        final byte[] actual = bytes();
        assertArrayEquals(
                expected, actual,
                "Bytes don't match!\nEXPECTED:\n" + hexDump(expected) + "\nACTUAL:\n" + hexDump(actual) + "\n"
        );
    }

    @Test
    public void testConstructorThrowsWhenBlockSizeTooSmall() {
        BlockAllocator ba = BlockAllocatorProviders.basicProvider().vendAllocator(9);
        assertThrows(IllegalArgumentException.class, () -> new WriteBuffer(ba, () -> {}));
    }

    @Test
    public void testInt8Positive()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i++)
        {
            buf.writeInt8(i);
            bytes[i] = (byte) i;
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt16Positive()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 2)
        {
            buf.writeInt16(i);
            bytes[i    ] = (byte) (i >> 8);
            bytes[i + 1] = (byte) (i >> 0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt32Positive()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 4)
        {
            buf.writeInt32(i);
            bytes[i    ] = (byte) (i >> 24);
            bytes[i + 1] = (byte) (i >> 16);
            bytes[i + 2] = (byte) (i >>  8);
            bytes[i + 3] = (byte) (i >>  0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt64Positive()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 8)
        {
            long val = i;
            buf.writeInt64(i);
            bytes[i    ] = (byte) (val >> 56);
            bytes[i + 1] = (byte) (val >> 48);
            bytes[i + 2] = (byte) (val >> 40);
            bytes[i + 3] = (byte) (val >> 32);
            bytes[i + 4] = (byte) (val >> 24);
            bytes[i + 5] = (byte) (val >> 16);
            bytes[i + 6] = (byte) (val >>  8);
            bytes[i + 7] = (byte) (val >>  0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt8Negative()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i ++)
        {
            long pos = i + 1;
            long neg = -pos;

            buf.writeInt8(neg);
            bytes[i] = (byte) (pos | 0x80);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt16Negative()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 2)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt16(neg);
            bytes[i    ] = (byte) ((pos >> 8) | 0x80);
            bytes[i + 1] = (byte) (pos >> 0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt24Negative()
    {
        final byte[] bytes = new byte[129];
        for (int i = 0; i < bytes.length; i += 3)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt24(neg);
            bytes[i    ] = (byte) ((pos >> 16) | 0x80);
            bytes[i + 1] = (byte) ( pos >>  8);
            bytes[i + 2] = (byte) ( pos >>  0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt32Negative()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 4)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt32(neg);
            bytes[i    ] = (byte) ((pos >> 24) | 0x80);
            bytes[i + 1] = (byte) (pos >> 16);
            bytes[i + 2] = (byte) (pos >>  8);
            bytes[i + 3] = (byte) (pos >>  0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt40Negative()
    {
        final byte[] bytes = new byte[160];
        for (int i = 0; i < bytes.length; i += 5)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt40(neg);
            bytes[i    ] = (byte) ((pos >> 32) | 0x80);
            bytes[i + 1] = (byte) (pos >>  24);
            bytes[i + 2] = (byte) (pos >>  16);
            bytes[i + 3] = (byte) (pos >>   8);
            bytes[i + 4] = (byte) (pos >>   0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt48Negative()
    {
        final byte[] bytes = new byte[144];
        for (int i = 0; i < bytes.length; i += 6)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt48(neg);
            bytes[i    ] = (byte) ((pos >> 40) | 0x80);
            bytes[i + 1] = (byte) (pos >>  32);
            bytes[i + 2] = (byte) (pos >>  24);
            bytes[i + 3] = (byte) (pos >>  16);
            bytes[i + 4] = (byte) (pos >>   8);
            bytes[i + 5] = (byte) (pos >>   0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testInt56Negative()
    {
        final byte[] bytes = new byte[168];
        for (int i = 0; i < bytes.length; i += 7)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt56(neg);
            bytes[i    ] = (byte) ((pos >> 48) | 0x80);
            bytes[i + 1] = (byte) (pos >>  40);
            bytes[i + 2] = (byte) (pos >>  32);
            bytes[i + 3] = (byte) (pos >>  24);
            bytes[i + 4] = (byte) (pos >>  16);
            bytes[i + 5] = (byte) (pos >>   8);
            bytes[i + 6] = (byte) (pos >>   0);
        }
        assertBuffer(bytes);
    }

    /**
     * Test if the method endOfBufferReached is invoked appropriately, the size of bytes written into the buffer overflow the current block size.
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testEndOfBufferReachedInvoked() throws UnsupportedEncodingException {
        buf.writeBytes("taco".getBytes("UTF-8"));
        buf.writeBytes("burrito".getBytes("UTF-8"));
        assertFalse(endOfBufferReached.get());
        buf.writeBytes("_".getBytes("UTF-8"));
        assertTrue(endOfBufferReached.get());
    }

    @Test
    public void testInt64Negative()
    {
        final byte[] bytes = new byte[128];
        for (int i = 0; i < bytes.length; i += 8)
        {
            long pos = i + 1;
            long neg = -pos;
            buf.writeInt64(neg);
            bytes[i    ] = (byte) ((pos >> 56) | 0x80);
            bytes[i + 1] = (byte) (pos >> 48);
            bytes[i + 2] = (byte) (pos >> 40);
            bytes[i + 3] = (byte) (pos >> 32);
            bytes[i + 4] = (byte) (pos >> 24);
            bytes[i + 5] = (byte) (pos >> 16);
            bytes[i + 6] = (byte) (pos >>  8);
            bytes[i + 7] = (byte) (pos >>  0);
        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt1()
    {
        final byte[] bytes = new byte[20];
        for (int i = 0; i < bytes.length; i++)
        {
            final int len = buf.writeVarUInt(0x7F);
            assertEquals(1, len);
            bytes[i] = (byte) 0xFF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt2()
    {
        final byte[] bytes = new byte[20];
        for (int i = 0; i < bytes.length; i += 2)
        {
            final int len = buf.writeVarUInt(0x3FFF);
            assertEquals(2, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0xFF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt3()
    {
        final byte[] bytes = new byte[21];
        for (int i = 0; i < bytes.length; i += 3)
        {
            final int len = buf.writeVarUInt(0x1FFFFF);
            assertEquals(3, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0xFF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt4()
    {
        final byte[] bytes = new byte[24];
        for (int i = 0; i < bytes.length; i += 4)
        {
            final int len = buf.writeVarUInt(0xFFFFFF0);
            assertEquals(4, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0xF0;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt5()
    {
        final byte[] bytes = new byte[25];
        for (int i = 0; i < bytes.length; i += 5)
        {
            final int len = buf.writeVarUInt(0x7FFFFFFF0L);
            assertEquals(5, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0xF0;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt6()
    {
        final byte[] bytes = new byte[30];
        for (int i = 0; i < bytes.length; i += 6)
        {
            final int len = buf.writeVarUInt(0x3FFFFFFFFF3L);
            assertEquals(6, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0xF3;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt7()
    {
        final byte[] bytes = new byte[35];
        for (int i = 0; i < bytes.length; i += 7)
        {
            final int len = buf.writeVarUInt(0x1FFFFFFFFFFF2L);
            assertEquals(7, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0xF2;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt8()
    {
        final byte[] bytes = new byte[40];
        for (int i = 0; i < bytes.length; i += 8)
        {
            final int len = buf.writeVarUInt(0xFFFFFFFFFFFFFAL);
            assertEquals(8, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0xFA;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt9()
    {
        final byte[] bytes = new byte[45];
        for (int i = 0; i < bytes.length; i += 9)
        {
            final int len = buf.writeVarUInt(0x7FFFFFFFFFFFFFFCL);
            assertEquals(9, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0x7F;
            bytes[i + 8] = (byte) 0xFC;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt1()
    {
        final byte[] bytes = new byte[40];
        for (int i = 0; i < bytes.length; i += 2)
        {
            int len = buf.writeVarInt(-0x3F);
            assertEquals(1, len);
            len = buf.writeVarInt(0x3F);
            assertEquals(1, len);
            bytes[i    ] = (byte) 0xFF;

            bytes[i + 1] = (byte) 0xBF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt2()
    {
        final byte[] bytes = new byte[40];
        for (int i = 0; i < bytes.length; i += 4)
        {
            int len = buf.writeVarInt(-0x1FFF);
            assertEquals(2, len);
            len = buf.writeVarInt(0x1FFF);
            assertEquals(2, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0xFF;

            bytes[i + 2] = (byte) 0x3F;
            bytes[i + 3] = (byte) 0xFF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt3()
    {
        final byte[] bytes = new byte[42];
        for (int i = 0; i < bytes.length; i += 6)
        {
            int len = buf.writeVarInt(-0xFFFFF);
            assertEquals(3, len);
            len = buf.writeVarInt(0xFFFFF);
            assertEquals(3, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0xFF;

            bytes[i + 3] = (byte) 0x3F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0xFF;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt4()
    {
        final byte[] bytes = new byte[48];
        for (int i = 0; i < bytes.length; i += 8)
        {
            int len = buf.writeVarInt(-0x7FFFFF0);
            assertEquals(4, len);
            len = buf.writeVarInt(0x7FFFFF0);
            assertEquals(4, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0xF0;

            bytes[i + 4] = (byte) 0x3F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0xF0;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt5()
    {
        final byte[] bytes = new byte[50];
        for (int i = 0; i < bytes.length; i += 10)
        {
            int len = buf.writeVarInt(-0x3FFFFFFF0L);
            assertEquals(5, len);
            len = buf.writeVarInt(0x3FFFFFFF0L);
            assertEquals(5, len);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0xF0;

            bytes[i + 5] = (byte) 0x3F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0x7F;
            bytes[i + 8] = (byte) 0x7F;
            bytes[i + 9] = (byte) 0xF0;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt6()
    {
        final byte[] bytes = new byte[60];
        for (int i = 0; i < bytes.length; i += 12)
        {
            int len = buf.writeVarInt(-0x1FFFFFFFFF3L);
            assertEquals(6, len);
            buf.writeVarInt(0x1FFFFFFFFF3L);
            assertEquals(6, len);
            bytes[i     ] = (byte) 0x7F;
            bytes[i +  1] = (byte) 0x7F;
            bytes[i +  2] = (byte) 0x7F;
            bytes[i +  3] = (byte) 0x7F;
            bytes[i +  4] = (byte) 0x7F;
            bytes[i +  5] = (byte) 0xF3;

            bytes[i +  6] = (byte) 0x3F;
            bytes[i +  7] = (byte) 0x7F;
            bytes[i +  8] = (byte) 0x7F;
            bytes[i +  9] = (byte) 0x7F;
            bytes[i + 10] = (byte) 0x7F;
            bytes[i + 11] = (byte) 0xF3;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt7()
    {
        final byte[] bytes = new byte[70];
        for (int i = 0; i < bytes.length; i += 14)
        {
            int len = buf.writeVarInt(-0xFFFFFFFFFFF2L);
            assertEquals(7, len);
            len = buf.writeVarInt(0xFFFFFFFFFFF2L);
            assertEquals(7, len);
            bytes[i     ] = (byte) 0x7F;
            bytes[i +  1] = (byte) 0x7F;
            bytes[i +  2] = (byte) 0x7F;
            bytes[i +  3] = (byte) 0x7F;
            bytes[i +  4] = (byte) 0x7F;
            bytes[i +  5] = (byte) 0x7F;
            bytes[i +  6] = (byte) 0xF2;

            bytes[i +  7] = (byte) 0x3F;
            bytes[i +  8] = (byte) 0x7F;
            bytes[i +  9] = (byte) 0x7F;
            bytes[i + 10] = (byte) 0x7F;
            bytes[i + 11] = (byte) 0x7F;
            bytes[i + 12] = (byte) 0x7F;
            bytes[i + 13] = (byte) 0xF2;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt8()
    {
        final byte[] bytes = new byte[80];
        for (int i = 0; i < bytes.length; i += 16)
        {
            int len = buf.writeVarInt(-0x7FFFFFFFFFFFFAL);
            assertEquals(8, len);
            len = buf.writeVarInt(0x7FFFFFFFFFFFFAL);
            assertEquals(8, len);
            bytes[i +  0] = (byte) 0x7F;
            bytes[i +  1] = (byte) 0x7F;
            bytes[i +  2] = (byte) 0x7F;
            bytes[i +  3] = (byte) 0x7F;
            bytes[i +  4] = (byte) 0x7F;
            bytes[i +  5] = (byte) 0x7F;
            bytes[i +  6] = (byte) 0x7F;
            bytes[i +  7] = (byte) 0xFA;

            bytes[i +  8] = (byte) 0x3F;
            bytes[i +  9] = (byte) 0x7F;
            bytes[i + 10] = (byte) 0x7F;
            bytes[i + 11] = (byte) 0x7F;
            bytes[i + 12] = (byte) 0x7F;
            bytes[i + 13] = (byte) 0x7F;
            bytes[i + 14] = (byte) 0x7F;
            bytes[i + 15] = (byte) 0xFA;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarInt9()
    {
        final byte[] bytes = new byte[90];
        for (int i = 0; i < bytes.length; i += 18)
        {
            int len = buf.writeVarInt(-0x3FFFFFFFFFFFFFFCL);
            assertEquals(9, len);
            len = buf.writeVarInt(0x3FFFFFFFFFFFFFFCL);
            assertEquals(9, len);
            bytes[i     ] = (byte) 0x7F;
            bytes[i +  1] = (byte) 0x7F;
            bytes[i +  2] = (byte) 0x7F;
            bytes[i +  3] = (byte) 0x7F;
            bytes[i +  4] = (byte) 0x7F;
            bytes[i +  5] = (byte) 0x7F;
            bytes[i +  6] = (byte) 0x7F;
            bytes[i +  7] = (byte) 0x7F;
            bytes[i +  8] = (byte) 0xFC;

            bytes[i +  9] = (byte) 0x3F;
            bytes[i + 10] = (byte) 0x7F;
            bytes[i + 11] = (byte) 0x7F;
            bytes[i + 12] = (byte) 0x7F;
            bytes[i + 13] = (byte) 0x7F;
            bytes[i + 14] = (byte) 0x7F;
            bytes[i + 15] = (byte) 0x7F;
            bytes[i + 16] = (byte) 0x7F;
            bytes[i + 17] = (byte) 0xFC;

        }
        assertBuffer(bytes);
    }

    @Test
    public void testVarIntMaxValue() throws IOException
    {
        buf.writeVarInt(Long.MAX_VALUE);
        buf.writeVarInt(Long.MAX_VALUE);

        byte[] expected = new byte[20];
        int i = 0;
        for (int j = 0; j < 2; j++)
        {
            expected[i++] = (byte) 0x01;
            for (int k = 0; k < 8; k++)
            {
                expected[i++] = (byte) 0x7F;
            }
            expected[i++] = (byte) 0xFF;
        }
        assertBuffer(expected);
    }

    @Test
    public void testVarIntMinValuePlusOne() throws IOException
    {
        buf.writeVarInt(Long.MIN_VALUE + 1);
        buf.writeVarInt(Long.MIN_VALUE + 1);

        byte[] expected = new byte[20];
        int i = 0;
        for (int j = 0; j < 2; j++)
        {
            expected[i++] = (byte) 0x41;
            for (int k = 0; k < 8; k++)
            {
                expected[i++] = (byte) 0x7F;
            }
            expected[i++] = (byte) 0xFF;
        }
        assertBuffer(expected);
    }


    @Test
    public void testVarUInt1At()
    {
        // pad some obvious bits 0b10101010
        final byte[] bytes = new byte[20];
        Arrays.fill(bytes, (byte) 0xAA);
        buf.writeBytes(bytes);

        assertBuffer(bytes);

        buf.writeVarUIntDirect1At(5, 0x0F);
        bytes[5] = (byte) 0x8F;

        assertBuffer(bytes);

        // XXX force at boundary
        buf.writeVarUIntDirect1At(11, 0x7F);
        bytes[11] = (byte) 0xFF;

        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt2AtSmall()
    {
        // pad some obvious bits 0b10101010
        final byte[] bytes = new byte[20];
        Arrays.fill(bytes, (byte) 0xAA);
        buf.writeBytes(bytes);

        assertBuffer(bytes);

        buf.writeVarUIntDirect2At(5, 0x07);
        bytes[5] = (byte) 0x00;
        bytes[6] = (byte) 0x87;

        assertBuffer(bytes);

        // XXX force at boundary
        buf.writeVarUIntDirect2At(10, 0x7F);
        bytes[10] = (byte) 0x00;
        bytes[11] = (byte) 0xFF;

        assertBuffer(bytes);
    }

    @Test
    public void testVarUInt2AtLarge()
    {
        // pad some obvious bits 0b10101010
        final byte[] bytes = new byte[20];
        Arrays.fill(bytes, (byte) 0xAA);
        buf.writeBytes(bytes);

        assertBuffer(bytes);

        buf.writeVarUIntDirect2At(5, 0x00FF);
        bytes[5] = (byte) 0x01;
        bytes[6] = (byte) 0xFF;

        assertBuffer(bytes);

        // XXX force at boundary
        buf.writeVarUIntDirect2At(10, 0x3FFF);
        bytes[10] = (byte) 0x7F;
        bytes[11] = (byte) 0xFF;

        assertBuffer(bytes);
    }

    @Test
    public void testUTF8Ascii() throws IOException
    {
        final String text = "hello world";
        buf.writeUTF8(text);
        // XXX make sure we go over a block boundary on a separate call
        buf.writeUTF8(text);
        final byte[] expected = "hello worldhello world".getBytes("UTF-8");
        assertBuffer(expected);
    }

    @Test
    public void testUTF8TwoByte() throws IOException
    {
        final String text = "h\u00F4!";
        buf.writeUTF8(text);
        // XXX make sure we go over a block boundary on a separate call
        buf.writeUTF8(text);
        buf.writeUTF8(text);
        buf.writeUTF8(text);
        buf.writeUTF8(text);
        final byte[] expected = "h\u00F4!h\u00F4!h\u00F4!h\u00F4!h\u00F4!".getBytes("UTF-8");
        assertBuffer(expected);
    }

    @Test
    public void testUTF8ThreeByte() throws IOException
    {
        // katakana 'ha'
        buf.writeUTF8("\u30CF");
        // XXX make sure we go over a block boundary on a separate call
        // katakana 'ro'
        buf.writeUTF8("\u30ED World!!!!!!!!");
        final byte[] expected = "\u30cf\u30ed World!!!!!!!!".getBytes("UTF-8");
        assertBuffer(expected);
    }

    @Test
    public void testUTF8TwoToThreeByte() throws IOException
    {
        buf.writeUTF8("h\u00F4\u30CF");
        // XXX make sure we go over a block boundary on a separate call
        buf.writeUTF8("h\u00F4\u30CF\u30ED World!!!!!!!!");
        final byte[] expected = "h\u00F4\u30CFh\u00F4\u30CF\u30ED World!!!!!!!!".getBytes("UTF-8");
        assertBuffer(expected);
    }

    @Test
    public void testUTF8FourByte() throws IOException
    {
        // this is emoji character 'poo'
        final String text = "\uD83D\uDCA9";
        buf.writeUTF8(text);
        final byte[] expected = text.getBytes("UTF-8");
        assertBuffer(expected);
    }

    @Test
    public void testUTF8BadSurrogate() throws IOException
    {
        try
        {
            // unpaired high surrogate
            buf.writeUTF8("\uD83D ");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 2-byte character
            buf.writeUTF8("\u00F4\uD83D ");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 3-byte character
            buf.writeUTF8("\u30CF\uD83D ");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 4-byte character
            buf.writeUTF8("\uD83D\uDCA9\uD83D ");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired low surrogate
            buf.writeUTF8("\uDCA9");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 2-byte character
            buf.writeUTF8("\u00F4\uDCA9");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 3-byte character
            buf.writeUTF8("\u30CF\uDCA9");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}

        try
        {
            // unpaired high surrogate after 4-byte character
            buf.writeUTF8("\uD83D\uDCA9\uDCA9");
            fail("Expected error!");
        }
        catch (final IllegalArgumentException e) {}
    }

    @Test
    public void testBytes() throws IOException
    {
        buf.writeBytes("ARGLE".getBytes("UTF-8"));
        buf.writeBytes("FOO".getBytes("UTF-8"));
        // XXX make sure we straddle the block boundary
        buf.writeBytes("BARGLE".getBytes("UTF-8"));
        buf.writeBytes("DOO".getBytes("UTF-8"));
        assertBuffer("ARGLEFOOBARGLEDOO".getBytes("UTF-8"));
    }

    @Test
    public void testTruncate() throws IOException
    {
        buf.writeBytes("ARGLE".getBytes("UTF-8"));
        buf.truncate(3);
        // Check that the expected bytes are present
        assertBuffer("ARG".getBytes("UTF-8"));
        // ...and check that we can resume writing without any issues
        buf.writeBytes("LEFOOBARGLEDOO".getBytes("UTF-8"));
        assertBuffer("ARGLEFOOBARGLEDOO".getBytes("UTF-8"));
    }

    @Test
    public void testTruncateAcrossBlocks() throws IOException
    {
        buf.writeBytes("ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes("UTF-8"));
        buf.truncate(3);
        // Check that the expected bytes are present
        assertBuffer("ABC".getBytes("UTF-8"));
        // ...and check that we can resume writing without any issues
        buf.writeBytes("DEFGHIJKLMNOPQRSTUVWXYZ".getBytes("UTF-8"));
        assertBuffer("ABCDEFGHIJKLMNOPQRSTUVWXYZ".getBytes("UTF-8"));
    }

    @Test
    public void shiftBytesLeftWithinFirstBufferBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // All bytes are being shifted within the first buffer block.
        buf.writeBytes("01234567".getBytes());
        buf.shiftBytesLeft(4, 1);
        assertBuffer("0124567".getBytes());
    }

    @Test
    public void shiftBytesLeftWithinLastBufferBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // All bytes are being shifted within the last buffer block.
        buf.writeBytes("0123456789ABCDEF".getBytes());
        buf.shiftBytesLeft(3, 2);
        assertBuffer("0123456789ADEF".getBytes());
    }

    @Test
    public void shiftBytesLeftAcrossBufferBlocks() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // Some bytes are shifted to the previous block, some are shifted within
        // the last block.
        buf.writeBytes("0123456789ABCDEF".getBytes());
        buf.shiftBytesLeft(8, 3);
        assertBuffer("0123489ABCDEF".getBytes());
    }

    @Test
    public void shiftBytesLeftAcrossBufferBlocksExclusively() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // Unlike `shiftBytesLeftAcrossBufferBlocks`, EVERY byte that is shifted
        // in the buffer ends up in the previous block.
        buf.writeBytes("0123456789ABCDEF".getBytes());
        buf.shiftBytesLeft(5, 5);
        assertBuffer("012345BCDEF".getBytes());
    }

    @Test
    public void shiftBytesLeftAcrossBufferBlocksEmptyingLastBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The "B" is the first and only byte in the second block.
        // Shifting 1 byte left by one empties the last block.
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(1, 1);
        assertBuffer("0123456789B".getBytes());
    }

    @Test
    public void shiftEntireBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The buffer contains two full blocks
        buf.writeBytes("0123456789|ABCDEFGHIJ|".getBytes());
        // We shift an entire block left by the block size
        buf.shiftBytesLeft(11, 11);
        assertBuffer("ABCDEFGHIJ|".getBytes());
    }

    @Test
    public void shiftBytesLeftByMoreThanTheBlockSize() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // We have 5 blocks' worth of data
        buf.writeBytes("0123456789|0123456789|0123456789|0123456789|0123456789|".getBytes());
        // We can shift left amounts greater than the block size
        buf.shiftBytesLeft(24, 24);
        assertBuffer("01234569|0123456789|0123456789|".getBytes());
    }

    @Test
    public void shiftBytesLeftAcrossBufferBlocksShorteningNextToLastBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The "B" is the first and only byte in the second block.
        // Shifting 2 bytes left by two empties the last block and shortens the next-to-last block by 1 byte.
        // Following this operation, the next-to-last block becomes the last block.
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(2, 2);
        assertBuffer("01234567AB".getBytes());
    }

    @Test
    public void writingAfterLastBlockChanges() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The "B" is the first and only byte in the second block.
        // Shifting 2 bytes left by two empties the last block and shortens the next-to-last block by 1 byte.
        // Following this operation, the next-to-last block becomes the last block.
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(2, 2);
        assertBuffer("01234567AB".getBytes());
        // After shifting and changing the last block, we can still append data without issue.
        buf.writeBytes("CDE".getBytes());
        assertBuffer("01234567ABCDE".getBytes());
    }

    @Test
    public void updateLastBlock() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The "B" is the first and only byte in the second block.
        // Shifting 2 bytes left by two empties the last block and shortens the next-to-last block by 1 byte.
        // Following this operation, the next-to-last block becomes the last block.
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(2, 2);
        assertBuffer("01234567AB".getBytes());
        // We write some more data to the buffer. If the last block has been discarded correctly, we can do another
        // shift operation without getting corrupt data.
        buf.writeBytes("CDEFGH".getBytes());
        assertBuffer("01234567ABCDEFGH".getBytes());
        buf.shiftBytesLeft(4, 2);
        assertBuffer("01234567ABEFGH".getBytes());
    }

    @Test
    public void shiftNBytesLeftByMoreThanNBytes() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.writeBytes("0123456789AB".getBytes());
        // Shift left by more bytes than we're shifting (shiftBy > length)
        buf.shiftBytesLeft(2, 5);
        assertBuffer("01234AB".getBytes());
    }

    @Test
    public void shiftLeftToBeginning() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.writeBytes("01234567AB".getBytes());
        buf.shiftBytesLeft(2, 8);
        assertBuffer("AB".getBytes());
    }

    @Test
    public void shiftLeftToBeginningAcrossBlocks() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(2, 10);
        assertBuffer("AB".getBytes());
    }

    @Test
    public void shiftBytesLeftAcrossBufferBlocksToStartThenWritePastBlockBoundary() throws IOException {
        assertEquals(11, ALLOCATOR.getBlockSize());
        // The "B" is the first and only byte in the second block.
        // Shifting 2 bytes left by 10 empties the last block and positions the shifted bytes at the start of the first
        // block.
        buf.writeBytes("0123456789AB".getBytes());
        buf.shiftBytesLeft(2, 10);
        assertBuffer("AB".getBytes());
        // Write enough bytes to expand back into the next block.
        buf.writeBytes("CDE0123456789AB".getBytes());
        assertBuffer("ABCDE0123456789AB".getBytes());
        // Shift one byte left past the block boundary. This should once again empty the last block.
        buf.shiftBytesLeft(1, 6);
        assertBuffer("ABCDE01234B".getBytes());
    }

    @Test
    public void shiftBytesLeftWithLengthZero() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.writeBytes("012345".getBytes());
        // Shift left by two, retaining zero bytes (i.e. truncate).
        // In Ion, this situation occurs when a length bytes are preallocated for containers that end up being empty.
        buf.shiftBytesLeft(0, 2);
        assertBuffer("0123".getBytes());
    }

    @Test
    public void shiftBytesLeftWithLengthZeroAcrossBlocks() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.writeBytes("0123456789|0".getBytes());
        // Shift left by two, retaining zero bytes (i.e. truncate).
        // In Ion, this situation occurs when a length bytes are preallocated for containers that end up being empty.
        buf.shiftBytesLeft(0, 2);
        assertBuffer("0123456789".getBytes());
    }

    @Test
    public void reserveShouldSkipTheRequestedNumberOfBytes() {
        buf.reserve(5);
        buf.writeBytes("A".getBytes());
        // WARNING: In testing, the reserved bytes do happen to be 0, but you cannot assume that is true in the general case.
        assertBuffer("\0\0\0\0\0A".getBytes());
    }

    @Test
    public void reserveShouldSkipTheRequestedNumberOfBytesAcrossOneBlock() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.reserve(15);
        buf.writeBytes("A".getBytes());
        // WARNING: In testing, the reserved bytes do happen to be 0, but you cannot assume that is true in the general case.
        assertBuffer("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0A".getBytes());
    }

    @Test
    public void reserveShouldSkipTheRequestedNumberOfBytesAcrossManyBlock() {
        assertEquals(11, ALLOCATOR.getBlockSize());
        buf.reserve(40);
        buf.writeBytes("A".getBytes());
        // WARNING: In testing, the reserved bytes do happen to be 0, but you cannot assume that is true in the general case.
        assertBuffer("\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0A".getBytes());
    }

    /**
     * Test if the method 'writeVarUIntTo' writes the expected bytes to the output stream.
     * @throws Exception if there is an error occurred while writing data to the output stream.
     */
    @Test
    public void writeVarUInt1() throws Exception {
        final byte[] bytes = new byte[20];
        for (int i = 0; i < bytes.length; i++)
        {
            writeVarUIntTo(out, 0x7F);
            bytes[i] = (byte) 0xFF;
        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt2() throws IOException {
        final byte[] bytes = new byte[20];
        for (int i = 0; i < bytes.length; i += 2)
        {
            writeVarUIntTo(out, 0x3FFF);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0xFF;
        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt3() throws IOException {
        final byte[] bytes = new byte[21];
        for (int i = 0; i < bytes.length; i += 3)
        {
            writeVarUIntTo(out, 0x1FFFFF);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0xFF;

        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt4() throws IOException {
        final byte[] bytes = new byte[24];
        for (int i = 0; i < bytes.length; i += 4)
        {
            writeVarUIntTo(out, 0xFFFFFF0);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0xF0;
        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt5() throws IOException {
        final byte[] bytes = new byte[25];
        for (int i = 0; i < bytes.length; i += 5)
        {
            writeVarUIntTo(out, 0x7FFFFFFF0L);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0xF0;

        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt6() throws IOException {
        final byte[] bytes = new byte[30];
        for (int i = 0; i < bytes.length; i += 6)
        {
            writeVarUIntTo(out, 0x3FFFFFFFFF3L);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0xF3;

        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt7() throws IOException {
        final byte[] bytes = new byte[35];
        for (int i = 0; i < bytes.length; i += 7)
        {
            writeVarUIntTo(out, 0x1FFFFFFFFFFF2L);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0xF2;

        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt8() throws IOException {
        final byte[] bytes = new byte[40];
        for (int i = 0; i < bytes.length; i += 8)
        {
            writeVarUIntTo(out, 0xFFFFFFFFFFFFFAL);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0xFA;

        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    @Test
    public void writeVarUInt9() throws IOException {
        final byte[] bytes = new byte[45];
        for (int i = 0; i < bytes.length; i += 9)
        {
            writeVarUIntTo(out, 0x7FFFFFFFFFFFFFFCL);
            bytes[i    ] = (byte) 0x7F;
            bytes[i + 1] = (byte) 0x7F;
            bytes[i + 2] = (byte) 0x7F;
            bytes[i + 3] = (byte) 0x7F;
            bytes[i + 4] = (byte) 0x7F;
            bytes[i + 5] = (byte) 0x7F;
            bytes[i + 6] = (byte) 0x7F;
            bytes[i + 7] = (byte) 0x7F;
            bytes[i + 8] = (byte) 0xFC;
        }
        assertArrayEquals(bytes, out.toByteArray());
    }

    /**
     * Test if the method 'varUIntLength' generates the expected length of the provided long value.
     */
    @Test
    public void testVarUIntLength1() {
        int length = varUIntLength(0x7F);
        assertEquals(1, length);
    }

    @Test
    public void testVarUIntLength2() {
        int length = varUIntLength(0x3FFF);
        assertEquals(2, length);
    }

    @Test
    public void testVarUIntLength3() {
        int length = varUIntLength(0x1FFFFF);
        assertEquals(3, length);
    }

    @Test
    public void testVarUIntLength4() {
        int length = varUIntLength(0xFFFFFF0);
        assertEquals(4, length);
    }

    @Test
    public void testVarUIntLength5() {
        int length = varUIntLength(0x7FFFFFFF0L);
        assertEquals(5, length);
    }

    @Test
    public void testVarUIntLength6() {
        int length = varUIntLength(0x3FFFFFFFFF3L);
        assertEquals(6, length);
    }

    @Test
    public void testVarUIntLength7() {
        int length = varUIntLength(0x1FFFFFFFFFFF2L);
        assertEquals(7, length);
    }

    @Test
    public void testVarUIntLength8() {
        int length = varUIntLength(0xFFFFFFFFFFFFFAL);
        assertEquals(8, length);
    }

    @Test
    public void testVarUIntLength9() {
        int length = varUIntLength(0x7FFFFFFFFFFFFFFCL);
        assertEquals(9, length);
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 00000001",
            "                   1, 00000011",
            "                   2, 00000101",
            "                   3, 00000111",
            "                   4, 00001001",
            "                   5, 00001011",
            "                  14, 00011101",
            "                  63, 01111111",
            "                  64, 00000010 00000001",
            "                 729, 01100110 00001011",
            "                8191, 11111110 01111111",
            "                8192, 00000100 00000000 00000001",
            "             1048575, 11111100 11111111 01111111",
            "             1048576, 00001000 00000000 00000000 00000001",
            "           134217727, 11111000 11111111 11111111 01111111",
            "           134217728, 00010000 00000000 00000000 00000000 00000001",
            "         17179869184, 00100000 00000000 00000000 00000000 00000000 00000001",
            "       2199023255552, 01000000 00000000 00000000 00000000 00000000 00000000 00000001",
            "     281474976710655, 11000000 11111111 11111111 11111111 11111111 11111111 01111111",
            "     281474976710656, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            "   36028797018963967, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            "   36028797018963968, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            // Different one-bits in every byte, making it easy to see if any bytes are out of order
            "   72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
            " 4611686018427387903, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            " 4611686018427387904, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            // Long.MAX_VALUE
            " 9223372036854775807, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 00000001",
            "                  -1, 11111111",
            "                  -2, 11111101",
            "                  -3, 11111011",
            "                 -14, 11100101",
            "                 -64, 10000001",
            "                 -65, 11111110 11111110",
            "                -729, 10011110 11110100",
            "               -8192, 00000010 10000000",
            "               -8193, 11111100 11111111 11111110",
            "            -1048576, 00000100 00000000 10000000",
            "            -1048577, 11111000 11111111 11111111 11111110",
            "          -134217728, 00001000 00000000 00000000 10000000",
            "          -134217729, 11110000 11111111 11111111 11111111 11111110",
            "        -17179869184, 00010000 00000000 00000000 00000000 10000000",
            "        -17179869185, 11100000 11111111 11111111 11111111 11111111 11111110",
            "    -281474976710656, 01000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "    -281474976710657, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            "  -36028797018963968, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "  -36028797018963969, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            // Different zero-bits in every byte, making it easy to see if any bytes are out of order
            "  -72624976668147841, 00000000 11111111 01111110 10111111 11011111 11101111 11110111 11111011 11111101",
            "-4611686018427387904, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "-4611686018427387905, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            // Long.MIN_VALUE
            "-9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 11111110",

    })
    public void testWriteFlexInt(long value, String expectedBits) {
        int numBytes = buf.writeFlexInt(value);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @Test
    public void testWriteFlexIntAcrossBlocks() {
        long value = Long.MIN_VALUE;
        String expectedNumberBits = "00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 11111110";

        for (int i = 0; i < ALLOCATOR.getBlockSize(); i++) {
            buf.reset();
            StringBuilder expectedBits = new StringBuilder();
            for (int j = 0; j < i; j++) {
                buf.writeByte((byte) 0x55);
                expectedBits.append("01010101 ");
            }
            expectedBits.append(expectedNumberBits);
            buf.writeFlexInt(value);
            String actualBits = byteArrayToBitString(bytes());
            Assertions.assertEquals(expectedBits.toString(), actualBits);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "                 0, 00000001",
            "                 1, 00000011",
            "                 2, 00000101",
            "                 3, 00000111",
            "                 4, 00001001",
            "                 5, 00001011",
            "                14, 00011101",
            "                63, 01111111",
            "                64, 10000001",
            "               127, 11111111",
            "               128, 00000010 00000010",
            "               729, 01100110 00001011",
            "             16383, 11111110 11111111",
            "             16384, 00000100 00000000 00000010",
            "           2097151, 11111100 11111111 11111111",
            "           2097152, 00001000 00000000 00000000 00000010",
            "         268435455, 11111000 11111111 11111111 11111111",
            "         268435456, 00010000 00000000 00000000 00000000 00000010",
            "       34359738368, 00100000 00000000 00000000 00000000 00000000 00000010",
            "     4398046511104, 01000000 00000000 00000000 00000000 00000000 00000000 00000010",
            "   562949953421311, 11000000 11111111 11111111 11111111 11111111 11111111 11111111",
            "   562949953421312, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
            " 72057594037927935, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
            " 72057594037927936, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
            // Different one-bits in every byte, making it easy to see if any bytes are out of order
            " 72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
            // Long.MAX_VALUE
            "9223372036854775807, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
    })
    public void testWriteFlexUInt(long value, String expectedBits) {
        int numBytes = buf.writeFlexUInt(value);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @Test
    public void testWriteFlexUIntAcrossBlocks() {
        long value = Long.MAX_VALUE;
        String expectedNumberBits = "00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111";

        for (int i = 0; i < ALLOCATOR.getBlockSize(); i++) {
            buf.reset();
            StringBuilder expectedBits = new StringBuilder();
            for (int j = 0; j < i; j++) {
                buf.writeByte((byte) 0x55);
                expectedBits.append("01010101 ");
            }
            expectedBits.append(expectedNumberBits);
            buf.writeFlexUInt(value);
            String actualBits = byteArrayToBitString(bytes());
            Assertions.assertEquals(expectedBits.toString(), actualBits);
        }
    }

    @Test
    public void testWriteFlexUIntForNegativeNumber() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> buf.writeFlexUInt(-1));
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 00000001",
            "                   1, 00000011",
            "                   2, 00000101",
            "                   3, 00000111",
            "                   4, 00001001",
            "                   5, 00001011",
            "                  14, 00011101",
            "                  63, 01111111",
            "                  64, 00000010 00000001",
            "                 729, 01100110 00001011",
            "                8191, 11111110 01111111",
            "                8192, 00000100 00000000 00000001",
            "             1048575, 11111100 11111111 01111111",
            "             1048576, 00001000 00000000 00000000 00000001",
            "           134217727, 11111000 11111111 11111111 01111111",
            "           134217728, 00010000 00000000 00000000 00000000 00000001",
            "         17179869184, 00100000 00000000 00000000 00000000 00000000 00000001",
            "       2199023255552, 01000000 00000000 00000000 00000000 00000000 00000000 00000001",
            "     281474976710655, 11000000 11111111 11111111 11111111 11111111 11111111 01111111",
            "     281474976710656, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            "   36028797018963967, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            "   36028797018963968, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            // Different one-bits in every byte, making it easy to see if any bytes are out of order
            "   72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
            " 4611686018427387903, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            " 4611686018427387904, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            // Long.MAX_VALUE
            " 9223372036854775807, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 00000001",
            " 9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
            "                  -1, 11111111",
            "                  -2, 11111101",
            "                  -3, 11111011",
            "                 -14, 11100101",
            "                 -64, 10000001",
            "                 -65, 11111110 11111110",
            "                -729, 10011110 11110100",
            "               -8192, 00000010 10000000",
            "               -8193, 11111100 11111111 11111110",
            "            -1048576, 00000100 00000000 10000000",
            "            -1048577, 11111000 11111111 11111111 11111110",
            "          -134217728, 00001000 00000000 00000000 10000000",
            "          -134217729, 11110000 11111111 11111111 11111111 11111110",
            "        -17179869184, 00010000 00000000 00000000 00000000 10000000",
            "        -17179869185, 11100000 11111111 11111111 11111111 11111111 11111110",
            "    -281474976710656, 01000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "    -281474976710657, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            "  -36028797018963968, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "  -36028797018963969, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            // Different zero-bits in every byte, making it easy to see if any bytes are out of order
            "  -72624976668147841, 00000000 11111111 01111110 10111111 11011111 11101111 11110111 11111011 11111101",
            "-4611686018427387904, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "-4611686018427387905, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111110",
            // Long.MIN_VALUE
            "-9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 11111110",
            "-9223372036854775809, 00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111101",
    })
    public void testWriteFlexIntForBigInteger(String value, String expectedBits) {
        int numBytes = buf.writeFlexInt(new BigInteger(value));
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @Test
    public void testWriteFlexIntForBigIntegerAcrossBlocks() {
        BigInteger value = new BigInteger("-9223372036854775809");
        String expectedNumberBits = "00000000 11111110 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111101";

        for (int i = 0; i < ALLOCATOR.getBlockSize(); i++) {
            buf.reset();
            StringBuilder expectedBits = new StringBuilder();
            for (int j = 0; j < i; j++) {
                buf.writeByte((byte) 0x55);
                expectedBits.append("01010101 ");
            }
            expectedBits.append(expectedNumberBits);
            buf.writeFlexInt(value);
            String actualBits = byteArrayToBitString(bytes());
            Assertions.assertEquals(expectedBits.toString(), actualBits);
        }
    }

    @ParameterizedTest
    @CsvSource({
            "                 0, 00000001",
            "                 1, 00000011",
            "                 2, 00000101",
            "                 3, 00000111",
            "                 4, 00001001",
            "                 5, 00001011",
            "                14, 00011101",
            "                63, 01111111",
            "                64, 10000001",
            "               127, 11111111",
            "               128, 00000010 00000010",
            "               729, 01100110 00001011",
            "             16383, 11111110 11111111",
            "             16384, 00000100 00000000 00000010",
            "           2097151, 11111100 11111111 11111111",
            "           2097152, 00001000 00000000 00000000 00000010",
            "         268435455, 11111000 11111111 11111111 11111111",
            "         268435456, 00010000 00000000 00000000 00000000 00000010",
            "       34359738368, 00100000 00000000 00000000 00000000 00000000 00000010",
            "     4398046511104, 01000000 00000000 00000000 00000000 00000000 00000000 00000010",
            "   562949953421311, 11000000 11111111 11111111 11111111 11111111 11111111 11111111",
            "   562949953421312, 10000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
            " 72057594037927935, 10000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
            " 72057594037927936, 00000000 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
            // Different one-bits in every byte, making it easy to see if any bytes are out of order
            " 72624976668147840, 00000000 00000001 10000001 01000000 00100000 00010000 00001000 00000100 00000010",
            // Long.MAX_VALUE
            "9223372036854775807, 00000000 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
            "9223372036854775808, 00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010",
    })
    public void testWriteFlexUIntForBigInteger(String value, String expectedBits) {
        int numBytes = buf.writeFlexUInt(new BigInteger(value));
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @Test
    public void testWriteFlexUIntForBigIntegerAcrossBlocks() {
        BigInteger value = new BigInteger("9223372036854775808");
        String expectedNumberBits = "00000000 00000010 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000010";

        for (int i = 0; i < ALLOCATOR.getBlockSize(); i++) {
            buf.reset();
            StringBuilder expectedBits = new StringBuilder();
            for (int j = 0; j < i; j++) {
                buf.writeByte((byte) 0x55);
                expectedBits.append("01010101 ");
            }
            expectedBits.append(expectedNumberBits);
            buf.writeFlexUInt(value);
            String actualBits = byteArrayToBitString(bytes());
            Assertions.assertEquals(expectedBits.toString(), actualBits);
        }
    }

    @Test
    public void testWriteFlexUIntForNegativeBigInteger() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> buf.writeFlexUInt(BigInteger.ONE.negate()));
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 00000000",
            "                   1, 00000001",
            "                   2, 00000010",
            "                  14, 00001110",
            "                 127, 01111111",
            "                 128, 10000000 00000000",
            "               32767, 11111111 01111111",
            "               32768, 00000000 10000000 00000000",
            "             3954261, 01010101 01010110 00111100",
            "             8388607, 11111111 11111111 01111111",
            "             8388608, 00000000 00000000 10000000 00000000",
            "          2147483647, 11111111 11111111 11111111 01111111",
            "          2147483648, 00000000 00000000 00000000 10000000 00000000",
            "        549755813887, 11111111 11111111 11111111 11111111 01111111",
            "        549755813888, 00000000 00000000 00000000 00000000 10000000 00000000",
            "     140737488355327, 11111111 11111111 11111111 11111111 11111111 01111111",
            "     140737488355328, 00000000 00000000 00000000 00000000 00000000 10000000 00000000",
            "   36028797018963967, 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            "   36028797018963968, 00000000 00000000 00000000 00000000 00000000 00000000 10000000 00000000",
            // Different one-bit in every byte, making it easy to see if any bytes are out of order
            "   72624976668147840, 10000000 01000000 00100000 00010000 00001000 00000100 00000010 00000001",
            // Long.MAX_VALUE
            " 9223372036854775807, 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            "                  -1, 11111111",
            "                  -2, 11111110",
            "                 -14, 11110010",
            "                -128, 10000000",
            "                -129, 01111111 11111111",
            "              -32768, 00000000 10000000",
            "              -32769, 11111111 01111111 11111111",
            "            -3954261, 10101011 10101001 11000011",
            "            -8388608, 00000000 00000000 10000000",
            "            -8388609, 11111111 11111111 01111111 11111111",
            "         -2147483648, 00000000 00000000 00000000 10000000",
            "         -2147483649, 11111111 11111111 11111111 01111111 11111111",
            "       -549755813888, 00000000 00000000 00000000 00000000 10000000",
            "       -549755813889, 11111111 11111111 11111111 11111111 01111111 11111111",
            "    -140737488355328, 00000000 00000000 00000000 00000000 00000000 10000000",
            "    -140737488355329, 11111111 11111111 11111111 11111111 11111111 01111111 11111111",
            "  -36028797018963968, 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
            "  -36028797018963969, 11111111 11111111 11111111 11111111 11111111 11111111 01111111 11111111",
            // Different zero-bit in every byte, making it easy to see if any bytes are out of order
            "  -72624976668147841, 01111111 10111111 11011111 11101111 11110111 11111011 11111101 11111110",
            // Long.MIN_VALUE
            "-9223372036854775808, 00000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",

    })
    public void testWriteFixedInt(long value, String expectedBits) {
        int numBytes = buf.writeFixedInt(value);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 00000000",
            "                   1, 00000001",
            "                   2, 00000010",
            "                  14, 00001110",
            "                 127, 01111111",
            "                 128, 10000000",
            "                 255, 11111111",
            "                 256, 00000000 00000001",
            "               65535, 11111111 11111111",
            "               65536, 00000000 00000000 00000001",
            "             3954261, 01010101 01010110 00111100",
            "            16777215, 11111111 11111111 11111111",
            "            16777216, 00000000 00000000 00000000 00000001",
            "          4294967295, 11111111 11111111 11111111 11111111",
            "          4294967296, 00000000 00000000 00000000 00000000 00000001",
            "       1099511627775, 11111111 11111111 11111111 11111111 11111111",
            "       1099511627776, 00000000 00000000 00000000 00000000 00000000 00000001",
            "     281474976710655, 11111111 11111111 11111111 11111111 11111111 11111111",
            "     281474976710656, 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            "    5023487023698435, 00000011 11010010 10010100 10110111 11010101 11011000 00010001",
            "   72057594037927935, 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
            "   72057594037927936, 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000001",
            // Different one-bit in every byte, making it easy to see if any bytes are out of order
            "   72624976668147840, 10000000 01000000 00100000 00010000 00001000 00000100 00000010 00000001",
            // Long.MAX_VALUE 72057594037927936
            " 9223372036854775807, 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
    })
    public void testWriteFixedUInt(long value, String expectedBits) {
        int numBytes = buf.writeFixedUInt(value);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @Test
    public void testWriteFixedUIntForNegativeNumber() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> buf.writeFixedUInt(-1));
    }

    @ParameterizedTest
    @CsvSource({
            "                   0, 1, 00000000",
            "                   0, 2, 00000000 00000000",
            "                   0, 8, 00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
            "                   1, 1, 00000001",
            "                   1, 2, 00000001 00000000",
            "                   1, 8, 00000001 00000000 00000000 00000000 00000000 00000000 00000000 00000000",
            "                 255, 1, 11111111",
            "                 255, 2, 11111111 00000000",
            "                 255, 3, 11111111 00000000 00000000",
            "                  -1, 1, 11111111",
            "                  -1, 2, 11111111 11111111",
            "                  -1, 8, 11111111 11111111 11111111 11111111 11111111 11111111 11111111 11111111",
            // Long.MIN_VALUE and Long.MAX_VALUE
            " 9223372036854775807, 8, 11111111 11111111 11111111 11111111 11111111 11111111 11111111 01111111",
            "-9223372036854775808, 8, 00000000 00000000 00000000 00000000 00000000 00000000 00000000 10000000",
    })
    public void testWriteFixedIntOrUInt(long value, int numBytes, String expectedBits) {
        int actualNumBytes = buf.writeFixedIntOrUInt(value, numBytes);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals(numBytes, actualNumBytes);
    }

    @Test
    public void testWriteFixedIntOrUIntThrowsExceptionWhenNumBytesIsOutOfBounds() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> buf.writeFixedIntOrUInt(0, -1));
        Assertions.assertThrows(IllegalArgumentException.class, () -> buf.writeFixedIntOrUInt(0, 9));
    }

    @ParameterizedTest
    @CsvSource({
            " 0, 00000001 01100000",
            " 1, 00000011",
            " 2, 00000101",
            "63, 01111111",
            "64, 00000010 00000001",
    })
    public void testWriteSidFlexSym(int value, String expectedBits) {
        int numBytes = buf.writeFlexSym(value);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    @ParameterizedTest
    @CsvSource({
            "'', 00000001 10000000", // 10000000 == SystemSymbols_1_1.EMPTY_TEXT.getId() converted to binary
            "a, 11111111 01100001",
            "abc, 11111011 01100001 01100010 01100011",
            "this is a very very very very very long symbol, " +
                    "10100101 01110100 01101000 01101001 01110011 00100000 01101001 01110011 00100000 01100001 00100000 " +
                    "01110110 01100101 01110010 01111001 00100000 01110110 01100101 01110010 01111001 00100000 01110110 " +
                    "01100101 01110010 01111001 00100000 01110110 01100101 01110010 01111001 00100000 01110110 01100101 " +
                    "01110010 01111001 00100000 01101100 01101111 01101110 01100111 00100000 01110011 01111001 01101101 " +
                    "01100010 01101111 01101100",
    })
    public void testWriteTextFlexSym(String value, String expectedBits) {
        // This is a sloppy way to construct a Result, but it works for this test because we only have ascii characters.
        Utf8StringEncoder.Result encoded = new Utf8StringEncoder.Result(value.length(), value.getBytes(StandardCharsets.US_ASCII));
        int numBytes = buf.writeFlexSym(encoded);
        String actualBits = byteArrayToBitString(bytes());
        Assertions.assertEquals(expectedBits, actualBits);
        Assertions.assertEquals((expectedBits.length() + 1)/9, numBytes);
    }

    /**
     * Converts a byte array to a string of bits, such as "00110110 10001001".
     * The purpose of this method is to make it easier to read and write test assertions.
     */
    private static String byteArrayToBitString(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (byte aByte : bytes) {
            for (int bit = 7; bit >= 0; bit--) {
                if (((0x01 << bit) & aByte) != 0) {
                    s.append("1");
                } else {
                    s.append("0");
                }
            }
            s.append(" ");
        }
        return s.toString().trim();
    }
}
