/*
 * Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.
 */
package com.amazon.ion.impl;

import com.amazon.ion.IonException;

/**
 * this class holds the various constants and helper functions Ion uses
 * to "understand" UTF-8 encoded input.  This may duplicate values that
 * are available in Java's Character class, but the goal is to fully
 * control and isolate the UTF-8 conversion here.
 *
 * The helper functions are intended to avoid the various bit twiddling
 * errors that can easily occur when working with bits.  As these functions
 * are all static final methods, and very short, the Java compilers should
 * find them very easy to in-line.  (in previous testing I have observed
 * that the Sun JVM heavily in-lines such methods).
 *
 * @author csuver
 * 21 April 2009
 *
 */
public class IonUTF8 {
    private final static int UNICODE_MAX_ONE_BYTE_SCALAR       = 0x0000007F; // 7 bits     =  7 / 1 = 7    bits per byte
    private final static int UNICODE_MAX_TWO_BYTE_SCALAR       = 0x000007FF; // 5 + 6 bits = 11 / 2 = 5.50 bits per byte
    private final static int UNICODE_MAX_THREE_BYTE_SCALAR     = 0x0000FFFF; // 4 + 6+6    = 16 / 3 = 5.33 bits per byte
    private final static int UNICODE_MAX_FOUR_BYTE_SCALAR      = 0x0010FFFF; // 3 + 6+6+6  = 21 / 4 = 5.25 bits per byte
    private final static int UNICODE_THREE_BYTES_OR_FEWER_MASK = 0xFFFF0000; // if any bits under the f's are set the scalar is either 4 bytes long, or invalid (negative or too large)
    private final static int UNICODE_TWO_BYTE_HEADER           = 0xC0;       // 8 + 4 = 12 = 0xC0
    private final static int UNICODE_THREE_BYTE_HEADER         = 0xE0;       // 8+4+2 = 14 = 0xE0
    private final static int UNICODE_FOUR_BYTE_HEADER          = 0xF0;       // 8+4+2+1 = 15 = 0xF0
    private final static int UNICODE_CONTINUATION_BYTE_HEADER  = 0x80;
    private final static int UNICODE_TWO_BYTE_MASK             = 0x1F;       // 8-3 = 5 bits
    private final static int UNICODE_THREE_BYTE_MASK           = 0x0F;       // 4 bits
    private final static int UNICODE_FOUR_BYTE_MASK            = 0x07;       // 3 bits
    private final static int UNICODE_CONTINUATION_BYTE_MASK    = 0x3F;       // 6 bits in each continuation char

    private final static int MAXIMUM_UTF16_1_CHAR_CODE_POINT   = 0x0000FFFF;
    private final static int SURROGATE_OFFSET                  = 0x00100000;
    private final static int SURROGATE_MASK                    = 0xFFFFFC00;  // 0b 1111 1100 0000 0000
    private final static int HIGH_SURROGATE                    = 0x0000D800;  // 0b 1101 1000 0000 0000
    private final static int LOW_SURROGATE                     = 0x0000DC00;  // 0b 1101 1100 0000 0000

    public final static boolean isHighSurrogate(int b) {
        return ((b & SURROGATE_MASK) == HIGH_SURROGATE);
    }
    public final static boolean isLowSurrogate(int b) {
        return ((b & SURROGATE_MASK) == LOW_SURROGATE);
    }
    public final static boolean isSurrogate(int b) {
        return (b >= 0xD800 && b <= 0xDFFF);  // 55296 to 57343 or 2048 chars in all
    }

    public final static boolean isOneByteUTF8(int b) {
        return ((b & 0x80) == 0);
    }
    public final static boolean isTwoByteUTF8(int b) {
        return ((b & ~UNICODE_TWO_BYTE_MASK) == UNICODE_TWO_BYTE_HEADER);
    }
    public final static boolean isThreeByteUTF8(int b) {
        return ((b & ~UNICODE_THREE_BYTE_MASK) == UNICODE_THREE_BYTE_HEADER);
    }
    public final static boolean isFourByteUTF8(int b) {
        return ((b & ~UNICODE_FOUR_BYTE_MASK) == UNICODE_FOUR_BYTE_HEADER);
    }
    public final static boolean isContinueByteUTF8(int b) {
        return ((b & ~UNICODE_CONTINUATION_BYTE_MASK) == UNICODE_CONTINUATION_BYTE_HEADER);
    }
    public final static boolean isStartByte(int b) {
        return isOneByteUTF8(b) || !isContinueByteUTF8(b);
    }

    public final static char twoByteScalar(int b1, int b2) {
        int c = ((b1 & UNICODE_TWO_BYTE_MASK) << 6) | (b2 & UNICODE_CONTINUATION_BYTE_MASK);
        return (char)c;
    }
    public final static int threeByteScalar(int b1, int b2, int b3) {
        int c = ((b1 & UNICODE_THREE_BYTE_MASK) << 12) | ((b2 & UNICODE_CONTINUATION_BYTE_MASK) << 6) | (b3 & UNICODE_CONTINUATION_BYTE_MASK);
        return c;
    }
    public final static int fourByteScalar(int b1, int b2, int b3, int b4) {
        int c = ((b1 & UNICODE_FOUR_BYTE_MASK) << 18) | ((b2 & UNICODE_CONTINUATION_BYTE_MASK) << 12) | ((b3 & UNICODE_CONTINUATION_BYTE_MASK) << 6) | (b4 & UNICODE_CONTINUATION_BYTE_MASK);
        return c;
    }

    public final static boolean isOneByteScalar(int unicodeScalar) {
        return (unicodeScalar <= UNICODE_MAX_ONE_BYTE_SCALAR);
    }
    public final static boolean isTwoByteScalar(int unicodeScalar) {
        return (unicodeScalar <= UNICODE_MAX_TWO_BYTE_SCALAR);
    }
    public final static boolean isThreeByteScalar(int unicodeScalar) {
        return (unicodeScalar <= UNICODE_MAX_THREE_BYTE_SCALAR);
    }
    public final static boolean isFourByteScalar(int unicodeScalar) {
        return (unicodeScalar <= UNICODE_MAX_FOUR_BYTE_SCALAR);
    }
    public final static int getUTF8ByteCount(int unicodeScalar) {
        if ((unicodeScalar & UNICODE_THREE_BYTES_OR_FEWER_MASK) != 0) {
            if (unicodeScalar >= 0 && unicodeScalar <= UNICODE_MAX_FOUR_BYTE_SCALAR)  return 4;
            throw new InvalidUnicodeCodePoint();
        }
        if (unicodeScalar <= UNICODE_MAX_ONE_BYTE_SCALAR)   return 1;
        if (unicodeScalar <= UNICODE_MAX_TWO_BYTE_SCALAR)   return 2;
        return 3;
    }
    public final static int getUTF8LengthFromFirstByte(int firstByte) {
        firstByte &= 0xff;
        if (isOneByteUTF8(firstByte))   return 1;
        if (isTwoByteUTF8(firstByte))   return 2;
        if (isThreeByteUTF8(firstByte)) return 3;
        if (isFourByteUTF8(firstByte))  return 4;
        throw new InvalidUnicodeCodePoint();
    }

    public final static byte getByte1Of2(int unicodeScalar) {
        int b1 = (UNICODE_TWO_BYTE_HEADER         | ((unicodeScalar >> 6)  & UNICODE_TWO_BYTE_MASK));
        return (byte)b1;
    }
    public final static byte getByte2Of2(int unicodeScalar) {
        int b2 = (UNICODE_CONTINUATION_BYTE_HEADER | ( unicodeScalar        & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b2;
    }
    public final static byte getByte1Of3(int unicodeScalar) {
        int b1 = (UNICODE_THREE_BYTE_HEADER        | ((unicodeScalar >> 12) & UNICODE_THREE_BYTE_MASK));
        return (byte)b1;
    }
    public final static byte getByte2Of3(int unicodeScalar) {
        int b2 = (UNICODE_CONTINUATION_BYTE_HEADER | ((unicodeScalar >> 6)  & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b2;
    }
    public final static byte getByte3Of3(int unicodeScalar) {
        int b3 = (UNICODE_CONTINUATION_BYTE_HEADER | ( unicodeScalar        & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b3;
    }
    public final static byte getByte1Of4(int unicodeScalar) {
        int b1 = (UNICODE_FOUR_BYTE_HEADER         | ((unicodeScalar >> 18) & UNICODE_FOUR_BYTE_MASK));
        return (byte)b1;
    }
    public final static byte getByte2Of4(int unicodeScalar) {
        int b2 = (UNICODE_CONTINUATION_BYTE_HEADER | ((unicodeScalar >> 12) & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b2;
    }
    public final static byte getByte3Of4(int unicodeScalar) {
        int b3 = (UNICODE_CONTINUATION_BYTE_HEADER | ((unicodeScalar >> 6)  & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b3;
    }
    public final static byte getByte4Of4(int unicodeScalar) {
        int b4 = (UNICODE_CONTINUATION_BYTE_HEADER | ( unicodeScalar        & UNICODE_CONTINUATION_BYTE_MASK));
        return (byte)b4;
    }
    public final static int getAs4BytesReversed(int unicodeScalar) {
        int four_bytes;

        //loop to write these bytes out:
        //bytes = getAs4Bytes(us)
        //do {
        //    write(bytes & 0xff);
        //    bytes = (bytes >>> 8);  // don't sign extend
        //} until (bytes == 0);

        switch (getUTF8ByteCount(unicodeScalar)) {
        case 1:
            return unicodeScalar;
        case 2:
            four_bytes  = getByte1Of2(unicodeScalar);
            four_bytes |= getByte2Of2(unicodeScalar) << 8;
            return four_bytes;
        case 3:
            four_bytes  = getByte1Of3(unicodeScalar);
            four_bytes |= getByte2Of3(unicodeScalar) << 8;
            four_bytes |= getByte3Of3(unicodeScalar) << 16;
            return four_bytes;
        case 4:
            four_bytes  = getByte1Of4(unicodeScalar);
            four_bytes |= getByte2Of4(unicodeScalar) << 8;
            four_bytes |= getByte3Of4(unicodeScalar) << 16;
            four_bytes |= getByte4Of4(unicodeScalar) << 24;
            return four_bytes;
        }
        throw new InvalidUnicodeCodePoint();
    }
    /**
     * this helper converts the unicodeScalar to a sequence of utf8 bytes
     * and copies those bytes into the supplied outputBytes array.  If there
     * is insufficient room in the array to hold the generated bytes it will
     * throw an ArrayIndexOutOfBoundsException.  It does not check for the
     * validity of the passed in unicodeScalar thoroughly, however it will
     * throw an InvalidUnicodeCodePoint if the value is less than negative
     * or the UTF8 encoding would exceed 4 bytes.
     * @param unicodeScalar scalar to convert
     * @param outputBytes user output array to fill with UTF8 bytes
     * @param offset first array element to fill
     * @param maxLength maximum number of array elements to fill
     * @return number of bytes written to the output array
     */
    public final static int convertToUTF8Bytes(int unicodeScalar, byte[] outputBytes, int offset, int maxLength)
    {
        int dst = offset;
        int end = offset + maxLength;

        switch (getUTF8ByteCount(unicodeScalar)) {
        case 1:
            if (dst >= end) throw new ArrayIndexOutOfBoundsException();
            outputBytes[dst++] = (byte)(unicodeScalar & 0xff);
            break;
        case 2:
            if (dst+1 >= end) throw new ArrayIndexOutOfBoundsException();
            outputBytes[dst++] = getByte1Of2(unicodeScalar);
            outputBytes[dst++] = getByte2Of2(unicodeScalar);
            break;
        case 3:
            if (dst+2 >= end) throw new ArrayIndexOutOfBoundsException();
            outputBytes[dst++] = getByte1Of3(unicodeScalar);
            outputBytes[dst++] = getByte2Of3(unicodeScalar);
            outputBytes[dst++] = getByte3Of3(unicodeScalar);
            break;
        case 4:
            if (dst+3 >= end) throw new ArrayIndexOutOfBoundsException();
            outputBytes[dst++] = getByte1Of4(unicodeScalar);
            outputBytes[dst++] = getByte2Of4(unicodeScalar);
            outputBytes[dst++] = getByte3Of4(unicodeScalar);
            outputBytes[dst++] = getByte4Of4(unicodeScalar);
            break;
        }
        return dst - offset;
    }
    /**
     * converts a unicode code point to a 0-3 bytes of UTF8
     * encoded data and a length - note this doesn't pack
     * a 1 byte character and it returns the start character.
     * this is the unpacking routine
     *  while (_utf8_pretch_byte_count > 0 && offset < limit) {
     *    _utf8_pretch_byte_count--;
     *    buffer[offset++] = (byte)((_utf8_pretch_bytes >> (_utf8_pretch_byte_count*8)) & 0xff);
     *  }
     */
    public final static int packBytesAfter1(int unicodeScalar, int utf8Len)
    {
        int packed_chars;

        switch (utf8Len) {
        default:
            throw new IllegalArgumentException("pack requires len > 1");
        case 2:
            packed_chars = getByte2Of2(unicodeScalar);
            break;
        case 3:
            packed_chars  = getByte2Of3(unicodeScalar);
            packed_chars |= getByte3Of3(unicodeScalar) << 8;
            break;
        case 4:
            packed_chars = getByte2Of4(unicodeScalar);
            packed_chars |= getByte3Of4(unicodeScalar) << 8;
            packed_chars |= getByte4Of4(unicodeScalar) << 16;
            break;
        }
        return packed_chars;

    }

    public final static int getScalarFrom4BytesReversed(int utf8BytesReversed)
    {
        //loop to read these bytes out:
        // int b4r = read();
        // switch (getUTF8LengthFromFirstByte(b)) {
        // case 4: b4r |= (read() << 24);  // fall through
        // case 3: b4r |= (read() << 16);
        // case 2: b4r |= (read() <<  8);
        // case 1: // nothing to do
        // }
        int c = utf8BytesReversed & 0xff;
        switch (getUTF8LengthFromFirstByte(c)) {
        case 1:
            return c;
        case 2:
            c  = ((c & UNICODE_TWO_BYTE_MASK) << 6);
            c |= ((utf8BytesReversed >> 8) & UNICODE_CONTINUATION_BYTE_MASK);
            return c;
        case 3:
            c  = ((c & UNICODE_THREE_BYTE_MASK) << 12);
            c |= ((utf8BytesReversed >> 2) & (UNICODE_CONTINUATION_BYTE_MASK) << 6);
            c |= ((utf8BytesReversed >> 16) & UNICODE_CONTINUATION_BYTE_MASK);
            return c;
        case 4:
            c  = ((c & UNICODE_FOUR_BYTE_MASK) << 18);
            c |= (((utf8BytesReversed << 4) & (UNICODE_CONTINUATION_BYTE_MASK) << 12));
            c |= (((utf8BytesReversed >> 2) & (UNICODE_CONTINUATION_BYTE_MASK) << 6));
            c |= ((utf8BytesReversed >> 24) & UNICODE_CONTINUATION_BYTE_MASK);
            return c;
        }
        throw new InvalidUnicodeCodePoint();
    }
    /**
     * this helper calculates the number of bytes it will consume from the
     * supplied byte array (bytes) if getScalarFromBytes is called with
     * the same parameters.  This is in place of having getScalarFromBytes
     * return an object with the bytes consumed and the resultant scalar.
     * This will throw ArrayIndexOutOfBoundsException if there are fewer
     * bytes remaining (maxLength) than needed for a valid UTF8 sequence
     * starting at offset.
     * @param bytes UTF8 bytes in user supplied array
     * @param offset first array element to read from
     * @param maxLength maximum number of bytes to read from the array
     * @return number of bytes needed to decode the next scalar
     */
    public final static int getScalarReadLengthFromBytes(byte[] bytes, int offset, int maxLength)
    {
        int src = offset;
        int end = offset + maxLength;

        if (src >= end) throw new ArrayIndexOutOfBoundsException();
        int c = bytes[src++] & 0xff;
        int utf8length = getUTF8LengthFromFirstByte(c);
        if (src + utf8length > end) throw new ArrayIndexOutOfBoundsException();

        return utf8length;
    }
    /**
     * this helper converts the bytes starting at offset from UTF8 to a
     * Unicode scalar.  This does not check for valid Unicode scalar ranges
     * but simply handle the UTF8 decoding.  getScalarReadLengthFromBytes
     * can be used to determine how many bytes would be converted (consumed) in
     * this process if the same parameters are passed in.  This will throw
     * ArrayIndexOutOfBoundsException if the array has too few bytes to
     * fully decode the scalar. It will throw InvalidUnicodeCodePoint if
     * the first byte isn't a valid UTF8 initial byte with a length of
     * 4 or less.
     * @param bytes UTF8 bytes in an array
     * @param offset initial array element to decode from
     * @param maxLength maximum number of bytes to consume from the array
     * @return Unicode scalar
     */
    public final static int getScalarFromBytes(byte[] bytes, int offset, int maxLength)
    {
        int src = offset;
        int end = offset + maxLength;

        if (src >= end) throw new ArrayIndexOutOfBoundsException();
        int c = bytes[src++] & 0xff;
        int utf8length = getUTF8LengthFromFirstByte(c);
        if (src + utf8length > end) throw new ArrayIndexOutOfBoundsException();

        switch (utf8length) {
        case 1:
            break;
        case 2:
            c  = (c & UNICODE_TWO_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            break;
        case 3:
            c  = (c & UNICODE_THREE_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            break;
        case 4:
            c  = (c & UNICODE_FOUR_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            c |= ((bytes[src++] & 0xff) & UNICODE_CONTINUATION_BYTE_MASK);
            break;
        default:
            throw new InvalidUnicodeCodePoint("code point is invalid: "+utf8length);
        }
        return c;
    }

    public final static boolean needsSurrogateEncoding(int unicodeScalar) {
        return (unicodeScalar > MAXIMUM_UTF16_1_CHAR_CODE_POINT);
    }
    public final static char highSurrogate(int unicodeScalar) {
        assert(unicodeScalar > MAXIMUM_UTF16_1_CHAR_CODE_POINT);
        assert(unicodeScalar <= Character.MAX_CODE_POINT);
        int c = ((unicodeScalar - SURROGATE_OFFSET) >> 10);
        return (char)((c | HIGH_SURROGATE) & 0xffff);
    }
    public final static char lowSurrogate(int unicodeScalar) {
        assert(unicodeScalar > MAXIMUM_UTF16_1_CHAR_CODE_POINT);
        assert(unicodeScalar <= Character.MAX_CODE_POINT);
        int c = ((unicodeScalar - SURROGATE_OFFSET) & 0x3ff);
        return (char)((c | LOW_SURROGATE) & 0xffff);
    }

    public static class InvalidUnicodeCodePoint extends IonException
    {
        private static final long serialVersionUID = -3200811216940328945L;

        public InvalidUnicodeCodePoint() {
            super("invlid UTF8");
        }
        public InvalidUnicodeCodePoint(String msg) {
            super(msg);
        }
        public InvalidUnicodeCodePoint(Exception e) {
            super(e);
        }
        public InvalidUnicodeCodePoint(String msg, Exception e) {
            super(msg, e);
        }
    }
}
