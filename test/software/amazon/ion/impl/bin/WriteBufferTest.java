/*
 * Copyright 2016-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static software.amazon.ion.TestUtils.hexDump;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.ion.impl.bin.BlockAllocator;
import software.amazon.ion.impl.bin.BlockAllocatorProviders;
import software.amazon.ion.impl.bin.WriteBuffer;

public class WriteBufferTest
{
    // XXX make this a prime to make it more likely that we collide on the edges of the buffer
    private static BlockAllocator ALLOCATOR = BlockAllocatorProviders.basicProvider().vendAllocator(11);

    private WriteBuffer buf;

    @Before
    public void setup()
    {
        buf = new WriteBuffer(ALLOCATOR);
    }

    @After
    public void teardown()
    {
        buf = null;
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
            "Bytes don't match!\nEXPECTED:\n" + hexDump(expected) + "\nACTUAL:\n" + hexDump(actual) + "\n",
            expected, actual
        );
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
}
