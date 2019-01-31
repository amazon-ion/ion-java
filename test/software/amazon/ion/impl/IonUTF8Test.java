package software.amazon.ion.impl;

import org.junit.Assert;
import org.junit.Test;

public class IonUTF8Test {

    @Test
    public void testGetScalarReadLengthFromBytes() {
        Assert.assertEquals(-1, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -8}, 0, 10));
        Assert.assertEquals(2, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -64}, 0, 20));
        Assert.assertEquals(1, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) 120}, 0, 20));
        Assert.assertEquals(3, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -32}, 0, 20));
        Assert.assertEquals(4, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -16}, 0, 20));
        Assert.assertEquals(-1, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -12, (byte) -128}, 1, 20));
        Assert.assertEquals(1, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) 112, (byte) 112}, 1, 20));
        Assert.assertEquals(4, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -16, (byte) -16}, 1, 20));
        Assert.assertEquals(3, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -32, (byte) -32}, 1, 20));
        Assert.assertEquals(2, IonUTF8.getScalarReadLengthFromBytes(new byte[] {(byte) -64, (byte) -63}, 0, 20));
    }

    @Test
    public void testGetScalarFromBytes() {
        Assert.assertEquals(112, IonUTF8.getScalarFromBytes(new byte[] {112}, 0, 33));
        Assert.assertEquals(9, IonUTF8.getScalarFromBytes(new byte[] {-63, -120, 20, 30}, 0, 3));
        Assert.assertEquals(9, IonUTF8.getScalarFromBytes(new byte[] {-31, -120, -64, 50} , 0, 8));
        Assert.assertEquals(17, IonUTF8.getScalarFromBytes(new byte[] {-15, -112, -128, -128}, 0, 8));
    }

    @Test
    public void testIsContinueByteUTF8() {
        Assert.assertEquals(true, IonUTF8.isContinueByteUTF8(128));
        Assert.assertEquals(false, IonUTF8.isContinueByteUTF8(0));
    }

    @Test
    public void testIsOneByteScalar() {
        Assert.assertEquals(true, IonUTF8.isOneByteScalar(-2147450752));
        Assert.assertEquals(false, IonUTF8.isOneByteScalar(128));
    }

    @Test
    public void testIsHighSurrogate() {
        Assert.assertEquals(true, IonUTF8.isHighSurrogate(55296));
        Assert.assertEquals(false, IonUTF8.isHighSurrogate(0));
    }

    @Test
    public void testGetScalarFrom4BytesReversed() {
        Assert.assertEquals(0, IonUTF8.getScalarFrom4BytesReversed(192));
        Assert.assertEquals(0, IonUTF8.getScalarFrom4BytesReversed(224));
        Assert.assertEquals(63, IonUTF8.getScalarFrom4BytesReversed((240<<24) - 64));
        Assert.assertEquals(0, IonUTF8.getScalarFrom4BytesReversed(192));
        Assert.assertEquals(0, IonUTF8.getScalarFrom4BytesReversed(240));
        Assert.assertEquals(0, IonUTF8.getScalarFrom4BytesReversed(0));
    }

    @Test
    public void testGetUnicodeScalarFromSurrogates() {
        Assert.assertEquals(458752, IonUTF8.getUnicodeScalarFromSurrogates(55680, 56320));
    }

    @Test
    public void testIsFourByteScalar() {
        Assert.assertEquals(true, IonUTF8.isFourByteScalar(-2146369535));
        Assert.assertEquals(false, IonUTF8.isFourByteScalar(1114113));
    }

    @Test
    public void testConvertToUTF8Bytes() {
        byte[] outBytes = new byte[] {13, 13, 13, 13};
        Assert.assertEquals(1, IonUTF8.convertToUTF8Bytes(32, outBytes, 0, 257));
        Assert.assertArrayEquals(new byte[] {32, 13, 13, 13}, outBytes);
        Assert.assertEquals(2, IonUTF8.convertToUTF8Bytes(1026, outBytes,  0, 257));
        Assert.assertArrayEquals(new byte[] {-48, -126, 13, 13}, outBytes);
        Assert.assertEquals(3, IonUTF8.convertToUTF8Bytes(3000, outBytes, 0, 257));
        Assert.assertArrayEquals(new byte[] {-32, -82, -72, 13}, outBytes);
        Assert.assertEquals(4, IonUTF8.convertToUTF8Bytes(65536, outBytes, 0, 257));
        Assert.assertArrayEquals(new byte[] {-16, -112, -128, -128}, outBytes);
    }

    @Test
    public void testIsFourByteUTF8() {
        Assert.assertEquals(true, IonUTF8.isFourByteUTF8(240));
        Assert.assertEquals(false, IonUTF8.isFourByteUTF8(0));
    }

    @Test
    public void testIsThreeByteScalar() {
        Assert.assertEquals(true, IonUTF8.isThreeByteScalar(-2147401728));
        Assert.assertEquals(false, IonUTF8.isThreeByteScalar(81920));
    }

    @Test
    public void testIsSurrogate() {
        Assert.assertEquals(false, IonUTF8.isSurrogate(57344));
        Assert.assertEquals(true, IonUTF8.isSurrogate(55300));
        Assert.assertEquals(false, IonUTF8.isSurrogate(0));
    }

    @Test
    public void testIsLowSurrogate() {
        Assert.assertEquals(true, IonUTF8.isLowSurrogate(56320));
        Assert.assertEquals(false, IonUTF8.isLowSurrogate(0));
    }

    @Test
    public void testGetAs4BytesReversed() {
        Assert.assertEquals(-48, IonUTF8.getAs4BytesReversed(1024));
        Assert.assertEquals(-24, IonUTF8.getAs4BytesReversed(32768));
        Assert.assertEquals(-16, IonUTF8.getAs4BytesReversed(66560));
        Assert.assertEquals(0, IonUTF8.getAs4BytesReversed(0));
    }

    @Test
    public void testTwoByteScalar() {
        Assert.assertEquals('\u0000', IonUTF8.twoByteScalar(0, 0));
    }

    @Test
    public void testIsTwoByteScalar() {
        Assert.assertEquals(true, IonUTF8.isTwoByteScalar(-2147481600));
        Assert.assertEquals(false, IonUTF8.isTwoByteScalar(2048));
    }

    @Test
    public void testIsTwoByteUTF8() {
        Assert.assertEquals(true, IonUTF8.isTwoByteUTF8(192));
        Assert.assertEquals(false, IonUTF8.isTwoByteUTF8(0));
    }

    @Test
    public void testGetUTF8ByteCount() {
        Assert.assertEquals(3, IonUTF8.getUTF8ByteCount(2048));
        Assert.assertEquals(4, IonUTF8.getUTF8ByteCount(67584));
        Assert.assertEquals(2, IonUTF8.getUTF8ByteCount(1024));
        Assert.assertEquals(1, IonUTF8.getUTF8ByteCount(0));
    }

    @Test
    public void testPackBytesAfter1() {
        Assert.assertEquals(-128, IonUTF8.packBytesAfter1(0, 2));
        Assert.assertEquals(-128, IonUTF8.packBytesAfter1(0, 3));
        Assert.assertEquals(-128, IonUTF8.packBytesAfter1(0, 4));
        Assert.assertEquals(-128, IonUTF8.packBytesAfter1(0, 3));
        Assert.assertEquals(-128, IonUTF8.packBytesAfter1(0, 4));
    }

    @Test
    public void testGetUTF8LengthFromFirstByte() {
        Assert.assertEquals(-1, IonUTF8.getUTF8LengthFromFirstByte(128));
        Assert.assertEquals(2, IonUTF8.getUTF8LengthFromFirstByte(192));
        Assert.assertEquals(4, IonUTF8.getUTF8LengthFromFirstByte(240));
        Assert.assertEquals(3, IonUTF8.getUTF8LengthFromFirstByte(224));
        Assert.assertEquals(1, IonUTF8.getUTF8LengthFromFirstByte(0));
    }

    @Test
    public void testIsOneByteUTF8() {
        Assert.assertEquals(false, IonUTF8.isOneByteUTF8(128));
        Assert.assertEquals(true, IonUTF8.isOneByteUTF8(0));
    }

    @Test
    public void testIsThreeByteUTF8() {
        Assert.assertEquals(true, IonUTF8.isThreeByteUTF8(224));
        Assert.assertEquals(false, IonUTF8.isThreeByteUTF8(0));
    }

    @Test
    public void testLowSurrogate() {
        Assert.assertEquals('\udc00', IonUTF8.lowSurrogate(73728));
        Assert.assertEquals('\udfc4', IonUTF8.lowSurrogate(131012));
    }

    @Test
    public void testHighSurrogate() {
        Assert.assertEquals('\ud808', IonUTF8.highSurrogate(73728));
        Assert.assertEquals('\ud83f', IonUTF8.highSurrogate(131012));
    }

    @Test
    public void testNeedsSurrogateEncoding() {
        Assert.assertEquals(true, IonUTF8.needsSurrogateEncoding(65536));
        Assert.assertEquals(false, IonUTF8.needsSurrogateEncoding(0));
    }

    @Test
    public void testIsStartByte() {
        Assert.assertEquals(true, IonUTF8.isStartByte(-2147483648));
        Assert.assertEquals(true, IonUTF8.isStartByte(-2147483520));
        Assert.assertEquals(false, IonUTF8.isStartByte(128));
        Assert.assertEquals(true, IonUTF8.isStartByte(0));
    }

    @Test
    public void testThreeByteScalar() {
        Assert.assertEquals(4227, IonUTF8.threeByteScalar(1, 2, 3));
    }

    @Test
    public void testFourByteScalar() {
        Assert.assertEquals(270532, IonUTF8.fourByteScalar(1, 2, 3, 4));
    }
}
