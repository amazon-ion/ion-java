// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;

import static com.amazon.ion.impl.bin.Ion_1_1_Constants.*;

/**
 * An IonCursor capable of raw parsing of binary Ion streams.
 */
class IonReaderContinuableCoreBinary extends IonCursorBinary implements IonReaderContinuableCore {

    // Isolates the highest bit in a byte.
    private static final int HIGHEST_BIT_BITMASK = 0x80;

    // Isolates the lowest seven bits in a byte.
    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;

    private static final int SINGLE_BYTE_MASK = 0xFF;
    private static final int TWO_BYTE_MASK = 0xFFFF;

    // Isolates the lowest six bits in a byte.
    private static final int LOWER_SIX_BITS_BITMASK = 0x3F;

    // The number of significant bits in each UInt byte.
    private static final int VALUE_BITS_PER_UINT_BYTE = 8;

    // The number of significant bits in each VarUInt byte.
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;

    // Single byte negative zero, represented as a VarInt. Often used in timestamp encodings to indicate unknown local
    // offset.
    private static final int VAR_INT_NEGATIVE_ZERO = 0xC0;

    // The number of bytes occupied by a Java int.
    private static final int INT_SIZE_IN_BYTES = 4;

    // The number of bytes occupied by a Java long.
    private static final int LONG_SIZE_IN_BYTES = 8;

    // The smallest negative 8-byte integer that can fit in a long is 0x80_00_00_00_00_00_00_00 and the smallest
    // negative 4-byte integer that can fit in an int is 0x80_00_00_00.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MIN_INTEGER = 0x80;

    // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF and the largest positive
    // 4-byte integer that can fit in an int is 0x7F_FF_FF_FF.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MAX_INTEGER = 0x7F;

    // The second-most significant bit in the most significant byte of a VarInt is the sign.
    private static final int VAR_INT_SIGN_BITMASK = 0x40;

    private static final int FLOAT_16_BYTE_LENGTH = 2;

    private static final int FLOAT_32_BYTE_LENGTH = 4;

    // Initial capacity of the ArrayList used to hold the symbol IDs of the annotations on the current value.
    private static final int ANNOTATIONS_LIST_INITIAL_CAPACITY = 8;

    // Converter between scalar types, allowing, for example, for a value encoded as an Ion float to be returned as a
    // Java `long` via `IonReader.longValue()`.
    private final _Private_ScalarConversions.ValueVariant scalarConverter;

    final Utf8StringDecoder utf8Decoder = Utf8StringDecoderPool.getInstance().getOrCreate();

    long peekIndex = -1;

    // The number of bytes of a lob value that the user has consumed, allowing for piecewise reads.
    private int lobBytesRead = 0;

    // The symbol IDs for the annotations on the current value.
    private final IntList annotationSids;

    /**
     * Constructs a new reader from the given byte array.
     * @param configuration the configuration to use. The buffer size and oversized value configuration are unused, as
     *                      the given buffer is used directly.
     * @param bytes the byte array containing the bytes to read.
     * @param offset the offset into the byte array at which the first byte of Ion data begins.
     * @param length the number of bytes to be read from the byte array.
     */
    IonReaderContinuableCoreBinary(IonBufferConfiguration configuration, byte[] bytes, int offset, int length) {
        super(configuration, bytes, offset, length);
        scalarConverter = new _Private_ScalarConversions.ValueVariant();
        annotationSids = new IntList(ANNOTATIONS_LIST_INITIAL_CAPACITY);
    }

    /**
     * Constructs a new reader from the given input stream.
     * @param configuration the configuration to use.
     * @param alreadyRead the byte array containing the bytes already read (often the IVM).
     * @param alreadyReadOff the offset into `alreadyRead` at which the first byte that was already read exists.
     * @param alreadyReadLen the number of bytes already read from `alreadyRead`.
     */
    IonReaderContinuableCoreBinary(IonBufferConfiguration configuration, InputStream inputStream, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen) {
        super(configuration, inputStream, alreadyRead, alreadyReadOff, alreadyReadLen);
        scalarConverter = new _Private_ScalarConversions.ValueVariant();
        annotationSids = new IntList(ANNOTATIONS_LIST_INITIAL_CAPACITY);
    }

    // Scratch space for various byte sizes. Only for use while computing a single value.
    private final byte[][] scratchForSize = new byte[][] {
        new byte[0],
        new byte[1],
        new byte[2],
        new byte[3],
        new byte[4],
        new byte[5],
        new byte[6],
        new byte[7],
        new byte[8],
        new byte[9],
        new byte[10],
        new byte[11],
        new byte[12],
    };

    /**
     * Returns a new or reused array of the requested size.
     * @param requestedSize the size of the scratch space to retrieve.
     * @return a byte array.
     */
    private byte[] getScratchForSize(int requestedSize) {
        byte[] bytes = null;
        if (requestedSize < scratchForSize.length) {
            bytes = scratchForSize[requestedSize];
        }
        if (bytes == null) {
            bytes = new byte[requestedSize];
        }
        return bytes;
    }

    /**
     * Copy the requested number of bytes from the buffer into a scratch buffer of exactly the requested length.
     * @param startIndex the start index from which to copy.
     * @param length the number of bytes to copy.
     * @return the scratch byte array.
     */
    private byte[] copyBytesToScratch(long startIndex, int length) {
        // Note: using reusable scratch buffers makes reading ints and decimals 1-5% faster and causes much less
        // GC churn.
        byte[] bytes = getScratchForSize(length);
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Reads the VarUInt starting at `peekIndex`. When this method returns, `peekIndex` will point at the first byte
     * that follows the VarUInt. NOTE: the VarUInt must fit in an `int`.
     * @return the value.
     */
    int readVarUInt_1_0() {
        int currentByte = 0;
        int result = 0;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
            currentByte = buffer[(int)(peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    /**
     * Reads a 2+ byte VarInt, given the first byte. When called, `peekIndex` must point at the second byte in the
     * VarInt. at `peekIndex`.When this method returns, `peekIndex` will point at the first byte that follows the
     * VarInt. NOTE: the VarInt must fit in an `int`.
     * @param firstByte the first byte of the VarInt representation, which has already been retrieved from the buffer.
     * @return the value.
     */
    private int readVarInt_1_0(int firstByte) {
        int currentByte = firstByte;
        int sign = (currentByte & VAR_INT_SIGN_BITMASK) == 0 ? 1 : -1;
        int result = currentByte & LOWER_SIX_BITS_BITMASK;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
            currentByte = buffer[(int)(peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result * sign;
    }

    /**
     * Reads the VarInt starting at `peekIndex`. When this method returns, `peekIndex` will point at the first byte
     * that follows the VarUInt. NOTE: the VarInt must fit in an `int`.
     * @return the value.
     */
    private int readVarInt_1_0() {
        return readVarInt_1_0(buffer[(int)(peekIndex++)]);
    }

    /**
     * Reads into a BigInteger the UInt value that begins at `valueMarker.startIndex` and ends at
     * `valueMarker.endIndex`.
     * @param isNegative true if the resulting BigInteger value should be negative; false if it should be positive.
     * @return the value.
     */
    private BigInteger readUIntAsBigInteger(boolean isNegative) {
        int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
        // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
        // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
        // lead to a significant performance improvement.
        byte[] magnitude = copyBytesToScratch(valueMarker.startIndex, length);
        int signum = isNegative ? -1 : 1;
        return new BigInteger(signum, magnitude);
    }

    /**
     * Get and clear the most significant bit in the given byte array.
     * @param intBytes bytes representing a signed int.
     * @return -1 if the most significant bit was set; otherwise, 1.
     */
    private int getAndClearSignBit_1_0(byte[] intBytes) {
        boolean isNegative = (intBytes[0] & HIGHEST_BIT_BITMASK) != 0;
        int signum = isNegative ? -1 : 1;
        if (isNegative) {
            intBytes[0] &= LOWER_SEVEN_BITS_BITMASK;
        }
        return signum;
    }

    /**
     * Reads the Int value of the given length that begins at `peekIndex` into a BigInteger.
     * @param length the length of the value.
     * @return the value.
     */
    private BigInteger readIntAsBigInteger_1_0(int length) {
        BigInteger value;
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
            // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
            // lead to a significant performance improvement.
            byte[] bytes = copyBytesToScratch(peekIndex, length);
            value = new BigInteger(getAndClearSignBit_1_0(bytes), bytes);
        }
        else {
            value = BigInteger.ZERO;
        }
        return value;
    }

    /**
     * Reads into a BigDecimal the decimal value that begins at `peekIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private BigDecimal readBigDecimal_1_0() {
        int scale = -readVarInt_1_0();
        BigDecimal value;
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (length < LONG_SIZE_IN_BYTES) {
            // No need to allocate a BigInteger to hold the coefficient.
            long coefficient = 0;
            int sign = 1;
            if (peekIndex < valueMarker.endIndex) {
                int firstByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
                sign = (firstByte & HIGHEST_BIT_BITMASK) == 0 ? 1 : -1;
                coefficient = firstByte & LOWER_SEVEN_BITS_BITMASK;
            }
            while (peekIndex < valueMarker.endIndex) {
                coefficient = (coefficient << VALUE_BITS_PER_UINT_BYTE) | buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
            }
            value = BigDecimal.valueOf(coefficient * sign, scale);
        } else {
            // The coefficient may overflow a long, so a BigInteger is required.
            value = new BigDecimal(readIntAsBigInteger_1_0(length), scale);
        }
        return value;
    }

    /**
     * Reads into a Decimal the decimal value that begins at `peekIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private Decimal readDecimal_1_0() {
        int scale = -readVarInt_1_0();
        BigInteger coefficient;
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor,
            // so copying to scratch space is always required.
            byte[] bits = copyBytesToScratch(peekIndex, length);
            int signum = getAndClearSignBit_1_0(bits);
            // NOTE: there is a BigInteger.valueOf(long unscaledValue, int scale) factory method that avoids allocating
            // a BigInteger for coefficients that fit in a long. See its use in readBigDecimal() above. Unfortunately,
            // it is not possible to use this for Decimal because the necessary BigDecimal constructor is
            // package-private. If a compatible BigDecimal constructor is added in a future JDK revision, a
            // corresponding factory method should be added to Decimal to enable this optimization.
            coefficient = new BigInteger(signum, bits);
            if (coefficient.signum() == 0 && signum < 0) {
                return Decimal.negativeZero(scale);
            }
        }
        else {
            coefficient = BigInteger.ZERO;
        }
        return Decimal.valueOf(coefficient, scale);
    }

    /**
     * Reads into a long the integer value that begins at `valueMarker.startIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private long readLong_1_0() {
        long value = readUInt(valueMarker.startIndex, valueMarker.endIndex);
        if (valueTid.isNegativeInt) {
            if (value == 0) {
                throw new IonException("Int zero may not be negative.");
            }
            value *= -1;
        }
        return value;
    }

    /**
     * Reads into a BigInteger the integer value that begins at `valueMarker.startIndex` and ends at
     * `valueMarker.endIndex`.
     * @return the value.
     */
    private BigInteger readBigInteger_1_0() {
        BigInteger value = readUIntAsBigInteger(valueTid.isNegativeInt);
        if (valueTid.isNegativeInt && value.signum() == 0) {
            throw new IonException("Int zero may not be negative.");
        }
        return value;
    }

    /**
     * Reads the timestamp that begins at `peekIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private Timestamp readTimestamp_1_0() {
        int firstByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        Integer offset = null;
        if (firstByte != VAR_INT_NEGATIVE_ZERO) {
            offset = readVarInt_1_0(firstByte);
        }
        int year = readVarUInt_1_0();
        int month = 1;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        BigDecimal fractionalSecond = null;
        Timestamp.Precision precision = Timestamp.Precision.YEAR;
        if (peekIndex < valueMarker.endIndex) {
            month = readVarUInt_1_0();
            precision = Timestamp.Precision.MONTH;
            if (peekIndex < valueMarker.endIndex) {
                day = readVarUInt_1_0();
                precision = Timestamp.Precision.DAY;
                if (peekIndex < valueMarker.endIndex) {
                    hour = readVarUInt_1_0();
                    if (peekIndex >= valueMarker.endIndex) {
                        throw new IonException("Timestamps may not specify hour without specifying minute.");
                    }
                    minute = readVarUInt_1_0();
                    precision = Timestamp.Precision.MINUTE;
                    if (peekIndex < valueMarker.endIndex) {
                        second = readVarUInt_1_0();
                        precision = Timestamp.Precision.SECOND;
                        if (peekIndex < valueMarker.endIndex) {
                            fractionalSecond = readBigDecimal_1_0();
                        }
                    }
                }
            }
        }
        try {
            return Timestamp.createFromUtcFields(
                precision,
                year,
                month,
                day,
                hour,
                minute,
                second,
                fractionalSecond,
                offset
            );
        } catch (IllegalArgumentException e) {
            throw new IonException("Illegal timestamp encoding. ", e);
        }
    }

    /**
     * Reads the boolean value using the type ID of the current value.
     * @return the value.
     */
    private boolean readBoolean_1_0() {
        return valueTid.lowerNibble == 1;
    }

    /**
     * Determines whether the integer starting at `valueMarker.startIndex` and ending at `valueMarker.endIndex`
     * crosses a type boundary. Callers must only invoke this method when the integer's size is known to be either
     * 4 or 8 bytes.
     * @return true if the value fits in the Java integer type that matches its Ion serialized size; false if it
     *  requires the next larger size.
     */
    private boolean classifyInteger_1_0() {
        if (valueTid.isNegativeInt) {
            int firstByte = buffer[(int)(valueMarker.startIndex)] & SINGLE_BYTE_MASK;
            if (firstByte < MOST_SIGNIFICANT_BYTE_OF_MIN_INTEGER) {
                return true;
            } else if (firstByte > MOST_SIGNIFICANT_BYTE_OF_MIN_INTEGER) {
                return false;
            }
            for (long i = valueMarker.startIndex + 1; i < valueMarker.endIndex; i++) {
                if (0x00 != buffer[(int)(i)]) {
                    return false;
                }
            }
            return true;
        }
        return (buffer[(int) (valueMarker.startIndex)] & SINGLE_BYTE_MASK) <= MOST_SIGNIFICANT_BYTE_OF_MAX_INTEGER;
    }

    /**
     * Reads a FlexUInt into an int. After this method returns, `peekIndex` points to the first byte after the end of
     * the FlexUInt.
     * @return the value.
     */
    long readFlexUInt_1_1() {
        int currentByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        byte length = (byte) (Integer.numberOfTrailingZeros(currentByte) + 1);
        long result = currentByte >>> length;
        for (byte i = 1; i < length; i++) {
            result |= ((long) (buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) << (8 * i - length));
        }
        return result;
    }

    private int readVarSym_1_1(Marker marker) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads a FixedInt into a long. After this method returns, `peekIndex` points to the first byte after the end of
     * the FixedInt.
     * @return the value.
     */
    private long readFixedInt_1_1() {
        if (peekIndex >= valueMarker.endIndex) {
            return 0;
        }
        long startIndex = peekIndex;
        peekIndex = valueMarker.endIndex;
        // Note: the following line performs sign extension via the cast to long without masking with 0xFF.
        long value = buffer[(int) --peekIndex];
        while (peekIndex > startIndex) {
            value = (value << 8) | (buffer[(int) --peekIndex] & SINGLE_BYTE_MASK);
        }
        peekIndex = valueMarker.endIndex;
        return value;
    }

    /**
     * Copies a FixedInt or FixedUInt into scratch space, converting it to its equivalent big-endian two's complement
     * representation. If the provided length is longer than the actual length of the value, the most significant
     * byte in the two's complement representation will be zero.
     * @param startIndex the index of the second byte in the FixedInt or FixedUInt representation.
     * @param length the number of bytes remaining in the FixedInt or FixedUInt representation.
     * @return a byte[] (either new or reused) containing the big-endian two's complement representation of the value.
     */
    private byte[] copyFixedIntOrFixedUIntAsTwosComplementBytes(long startIndex, int length) {
        // FixedInt is a little-endian two's complement representation. Simply reverse the bytes.
        byte[] bytes = getScratchForSize(length);
        // Clear the most significant byte in case the scratch space is padded to accommodate an unsigned value with
        // its highest bit set.
        bytes[0] = 0;
        int copyIndex = bytes.length;
        for (long i = startIndex; i < valueMarker.endIndex; i++) {
            bytes[--copyIndex] = buffer[(int) i];
        }
        peekIndex = valueMarker.endIndex;
        return bytes;
    }

    /**
     * Reads a FixedInt or FixedUInt value into a BigInteger.
     * @param length the length of the two's complement representation of the value. For FixedInts, this is always
     *               equal to the length of the value; for FixedUInts, this is one byte larger than the length of the
     *               value if the highest bit in the unsigned representation is set.
     * @return the value.
     */
     private BigInteger readFixedIntOrFixedUIntAsBigInteger_1_1(int length) {
         BigInteger value;
         if (length > 0) {
             value = new BigInteger(copyFixedIntOrFixedUIntAsTwosComplementBytes(peekIndex, length));
         } else {
             value = BigInteger.ZERO;
         }
         return value;
     }

    private BigDecimal readBigDecimal_1_1() {
        throw new UnsupportedOperationException();
    }

    private Decimal readDecimal_1_1() {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads the FixedInt bounded by `valueMarker` into a `long`.
     * @return the value.
     */
    private long readLong_1_1() {
        peekIndex = valueMarker.startIndex;
        return readFixedInt_1_1();
    }

    /**
     * Reads the FixedInt bounded by `valueMarker` into a BigInteger.
     * @return the value.
     */
    private BigInteger readBigInteger_1_1() {
        peekIndex = valueMarker.startIndex;
        return readFixedIntOrFixedUIntAsBigInteger_1_1((int) (valueMarker.endIndex - peekIndex));
    }

    /**
     * Reads the fraction component of an Ion 1.1 long form timestamp.
     * @return the value as a BigDecimal.
     */
    private BigDecimal readTimestampFraction_1_1() {
        // The fractional seconds are encoded as a (scale, coefficient) pair,
        // which is similar to a decimal. The primary difference is that the scale represents a negative
        // exponent because it is illegal for the fractional seconds value to be greater than or equal to 1.0
        // or less than 0.0. The coefficient is encoded as a FixedUInt (instead of FixedInt) to prevent the
        // encoding of fractional seconds less than 0.0. The scale is encoded as a FlexUInt (instead of FlexInt)
        // to discourage the encoding of decimal numbers greater than 1.0.
        BigDecimal value;
        peekIndex = valueMarker.startIndex + L_TIMESTAMP_SECOND_BYTE_LENGTH;
        int scale = (int) readFlexUInt_1_1();
        if (peekIndex >= valueMarker.endIndex) {
            return BigDecimal.valueOf(0, scale);
        }
        int length = (int) (valueMarker.endIndex - peekIndex);
        // Since the coefficient is stored in a FixedUInt, some 8-byte values cannot fit in a signed 8-byte long.
        // Take the quick path for values up to 7 bytes rather than performing additional checks. This should cover
        // almost all real-world timestamp fractions.
        if (length <= 7) {
            // No need to allocate a BigInteger to hold the coefficient.
            value = BigDecimal.valueOf(readFixedUInt_1_1(peekIndex, valueMarker.endIndex), scale);
        } else {
            // The coefficient may overflow a long, so a BigInteger is required.
            // If the most-significant bit is set, pad the length by one byte so that the value remains unsigned.
            length += (buffer[(int) valueMarker.endIndex - 1] < 0) ? 1 : 0;
            value = new BigDecimal(readFixedIntOrFixedUIntAsBigInteger_1_1(length), scale);
        }
        if (BigDecimal.ONE.compareTo(value) < 1) {
            throw new IllegalArgumentException(String.format("Fractional seconds %s must be greater than or equal to 0 and less than 1", value));
        }
        return value;
    }

    /**
     * Reads an Ion 1.1 long form timestamp.
     * @return the value.
     */
    private Timestamp readTimestampLongForm_1_1() {
        int year;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        BigDecimal fractionalSecond = null;
        boolean isOffsetUnknown = true;
        int offset = 0;
        int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
        if (length > L_TIMESTAMP_SECOND_BYTE_LENGTH) {
            // Fractional component.
            fractionalSecond = readTimestampFraction_1_1();
            length = L_TIMESTAMP_SECOND_BYTE_LENGTH;
        }
        Timestamp.Precision precision = L_TIMESTAMP_PRECISION_FOR_LENGTH[length];
        long bits = 0;
        for (int i = length - 1; i >= 0 ; i--) {
            bits = (bits << 8) | (buffer[i + (int) valueMarker.startIndex] & SINGLE_BYTE_MASK);
        }
        switch (length) {
            case L_TIMESTAMP_SECOND_BYTE_LENGTH:
                second = (int) ((bits & L_TIMESTAMP_SECOND_MASK) >>> L_TIMESTAMP_SECOND_BIT_OFFSET);
            case L_TIMESTAMP_MINUTE_BYTE_LENGTH:
                offset = (int) ((bits & L_TIMESTAMP_OFFSET_MASK) >>> L_TIMESTAMP_OFFSET_BIT_OFFSET);
                if ((offset ^ TWELVE_BIT_MASK) != 0) {
                    isOffsetUnknown = false;
                    offset -= L_TIMESTAMP_OFFSET_BIAS;
                }
                minute = (int) ((bits & L_TIMESTAMP_MINUTE_MASK) >>> L_TIMESTAMP_MINUTE_BIT_OFFSET);
                hour = (int) (bits & L_TIMESTAMP_HOUR_MASK) >>> L_TIMESTAMP_HOUR_BIT_OFFSET;
            case L_TIMESTAMP_DAY_OR_MONTH_BYTE_LENGTH:
                day = (int) (bits & L_TIMESTAMP_DAY_MASK) >>> L_TIMESTAMP_DAY_BIT_OFFSET;
                if (length == L_TIMESTAMP_DAY_OR_MONTH_BYTE_LENGTH) {
                    // Month and Day precision share the same length. If the day subfield is 0, the timestamp has
                    // month precision. Otherwise, it has day precision.
                    precision = day == 0 ? Timestamp.Precision.MONTH : Timestamp.Precision.DAY;
                }
                month = (int) (bits & L_TIMESTAMP_MONTH_MASK) >>> L_TIMESTAMP_MONTH_BIT_OFFSET;
            case L_TIMESTAMP_YEAR_BYTE_LENGTH:
                year = (int) (bits & L_TIMESTAMP_YEAR_MASK);
                break;
            default:
                throw new IonException("Illegal timestamp encoding.");
        }
        try {
            return Timestamp._private_createFromLocalTimeFieldsUnchecked(
                precision,
                year,
                month,
                day,
                hour,
                minute,
                second,
                fractionalSecond,
                isOffsetUnknown ? null : offset
            );
        } catch (IllegalArgumentException e) {
            throw new IonException("Illegal timestamp encoding. ", e);
        }
    }

    /**
     * Reads an Ion 1.1 timestamp in either the long or short form.
     * @return the value.
     */
    private Timestamp readTimestamp_1_1() {
        if (valueTid.variableLength) {
            return readTimestampLongForm_1_1();
        }
        Timestamp.Precision precision = S_TIMESTAMP_PRECISION_FOR_TYPE_ID_OFFSET[valueTid.lowerNibble];
        int year = 0;
        int month = 1;
        int day = 1;
        int hour = 0;
        int minute = 0;
        int second = 0;
        BigDecimal fractionalSecond = null;
        Integer offset = null;
        long bits = 0;
        for (int i = (int) Math.min(valueMarker.endIndex, valueMarker.startIndex + 8) - 1; i >= valueMarker.startIndex ; i--) {
            bits = (bits << 8) | (buffer[i] & SINGLE_BYTE_MASK);
        }
        switch (precision) {
            case FRACTION:
            case SECOND:
                int unscaledValue = -1;
                int scale = -1;
                int bound = -1;
                switch (valueTid.lowerNibble) {
                    case S_O_TIMESTAMP_NANOSECOND_LOWER_NIBBLE:
                        // The least-significant 24 bits of the nanoseconds field are contained in the long.
                        unscaledValue = (int) ((bits & S_O_TIMESTAMP_NANOSECOND_EIGHTH_BYTE_MASK) >>> S_O_TIMESTAMP_FRACTION_BIT_OFFSET);
                        // The most-significant 6 bits of the nanoseconds field are contained in the ninth byte.
                        unscaledValue |= (int) ((buffer[(int) valueMarker.endIndex - 1] & S_O_TIMESTAMP_NANOSECOND_NINTH_BYTE_MASK)) << S_O_TIMESTAMP_NANOSECOND_BITS_IN_EIGHTH_BYTE;
                        bound = MAX_NANOSECONDS;
                        scale = NANOSECOND_SCALE;
                        break;
                    case S_U_TIMESTAMP_NANOSECOND_LOWER_NIBBLE:
                        unscaledValue = (int) ((bits & S_U_TIMESTAMP_NANOSECOND_MASK) >>> S_U_TIMESTAMP_FRACTION_BIT_OFFSET);
                        bound = MAX_NANOSECONDS;
                        scale = NANOSECOND_SCALE;
                        break;
                    case S_O_TIMESTAMP_MICROSECOND_LOWER_NIBBLE:
                        unscaledValue = (int) ((bits & S_O_TIMESTAMP_MICROSECOND_MASK) >>> S_O_TIMESTAMP_FRACTION_BIT_OFFSET);
                        bound = MAX_MICROSECONDS;
                        scale = MICROSECOND_SCALE;
                        break;
                    case S_U_TIMESTAMP_MICROSECOND_LOWER_NIBBLE:
                        unscaledValue = (int) ((bits & S_U_TIMESTAMP_MICROSECOND_MASK) >>> S_U_TIMESTAMP_FRACTION_BIT_OFFSET);
                        bound = MAX_MICROSECONDS;
                        scale = MICROSECOND_SCALE;
                        break;
                    case S_O_TIMESTAMP_MILLISECOND_LOWER_NIBBLE:
                        unscaledValue = (int) ((bits & S_O_TIMESTAMP_MILLISECOND_MASK) >>> S_O_TIMESTAMP_FRACTION_BIT_OFFSET);
                        bound = MAX_MILLISECONDS;
                        scale = MILLISECOND_SCALE;
                        break;
                    case S_U_TIMESTAMP_MILLISECOND_LOWER_NIBBLE:
                        unscaledValue = (int) ((bits & S_U_TIMESTAMP_MILLISECOND_MASK) >>> S_U_TIMESTAMP_FRACTION_BIT_OFFSET);
                        bound = MAX_MILLISECONDS;
                        scale = MILLISECOND_SCALE;
                        break;
                    default:
                        // Second.
                        break;
                }
                if (unscaledValue >= 0) {
                    if (unscaledValue > bound) {
                        throw new IonException("Timestamp fraction must be between 0 and 1.");
                    }
                    fractionalSecond = BigDecimal.valueOf(unscaledValue, scale);
                }
                if (valueTid.lowerNibble >= S_O_TIMESTAMP_MINUTE_LOWER_NIBBLE) {
                    second = (int) ((bits & S_O_TIMESTAMP_SECOND_MASK) >>> S_O_TIMESTAMP_SECOND_BIT_OFFSET);
                } else {
                    second = (int) ((bits & S_U_TIMESTAMP_SECOND_MASK) >>> S_U_TIMESTAMP_SECOND_BIT_OFFSET);
                }
            case MINUTE:
                if (valueTid.lowerNibble >= S_O_TIMESTAMP_MINUTE_LOWER_NIBBLE) {
                    offset = (int) (((bits & S_O_TIMESTAMP_OFFSET_MASK) >>> S_O_TIMESTAMP_OFFSET_BIT_OFFSET) - S_O_TIMESTAMP_OFFSET_BIAS) * S_O_TIMESTAMP_OFFSET_INCREMENT;
                } else {
                    offset = (bits & S_U_TIMESTAMP_UTC_FLAG) == 0 ? null : 0;
                }
                minute = (int) (bits & S_TIMESTAMP_MINUTE_MASK) >>> S_TIMESTAMP_MINUTE_BIT_OFFSET;
                hour = (int) (bits & S_TIMESTAMP_HOUR_MASK) >>> S_TIMESTAMP_HOUR_BIT_OFFSET;
            case DAY:
                day = (int) (bits & S_TIMESTAMP_DAY_MASK) >>> S_TIMESTAMP_DAY_BIT_OFFSET;
            case MONTH:
                month = (int) (bits & S_TIMESTAMP_MONTH_MASK) >>> S_TIMESTAMP_MONTH_BIT_OFFSET;
            case YEAR:
                // Year is encoded as the number of years since 1970.
                year = S_TIMESTAMP_YEAR_BIAS + (int) (bits & S_TIMESTAMP_YEAR_MASK);
        }
        try {
            return Timestamp._private_createFromLocalTimeFieldsUnchecked(
                precision,
                year,
                month,
                day,
                hour,
                minute,
                second,
                fractionalSecond,
                offset
            );
        } catch (IllegalArgumentException e) {
            throw new IonException("Illegal timestamp encoding. ", e);
        }
    }

    /**
     * Reads the boolean value using the type ID of the current value.
     * @return the value.
     */
    private boolean readBoolean_1_1() {
        // Boolean 'true' is 0x5E; 'false' is 0x5F.
        return valueTid.lowerNibble == 0xE;
    }

    @Override
    public Event nextValue() {
        lobBytesRead = 0;
        return super.nextValue();
    }

    /**
     * Prepares the ByteBuffer to wrap a slice of the underlying buffer.
     * @param startIndex the start of the slice.
     * @param endIndex the end of the slice.
     * @return the ByteBuffer.
     */
    ByteBuffer prepareByteBuffer(long startIndex, long endIndex) {
        // Setting the limit to the capacity first is required because setting the position will fail if the new
        // position is outside the limit.
        byteBuffer.limit(buffer.length);
        byteBuffer.position((int) startIndex);
        byteBuffer.limit((int) endIndex);
        return byteBuffer;
    }


    /**
     * Reads a UInt (big-endian).
     * @param startIndex the index of the first byte in the UInt value.
     * @param endIndex the index of the first byte after the end of the UInt value.
     * @return the value.
     */
    private long readUInt(long startIndex, long endIndex) {
        long result = 0;
        for (long i = startIndex; i < endIndex; i++) {
            result = (result << VALUE_BITS_PER_UINT_BYTE) | buffer[(int) i] & SINGLE_BYTE_MASK;
        }
        return result;
    }

    /**
     * Reads a FixedUInt (little-endian).
     * @param startIndex the index of the first byte in the FixedUInt value.
     * @param endIndex the index of the first byte after the end of the FixedUInt value.
     * @return the value.
     */
    private long readFixedUInt_1_1(long startIndex, long endIndex) {
        long result = 0;
        for (int i = (int) startIndex; i < endIndex; i++) {
            result |= ((long) (buffer[i] & SINGLE_BYTE_MASK) << ((i - startIndex) * VALUE_BITS_PER_UINT_BYTE));
        }
        return result;
    }

    @Override
    public boolean isNullValue() {
        return valueTid != null && valueTid.isNull;
    }

    /**
     * Performs any logic necessary to prepare a scalar value for parsing. Subclasses may wish to provide additional
     * logic, such as ensuring that the value is present in the buffer.
     */
    void prepareScalar() {
        if (valueMarker.endIndex > limit) {
            throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
        }
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (valueTid == null || valueTid.type != IonType.INT || valueTid.isNull) {
            return null;
        }
        prepareScalar();
        int length = valueTid.variableLength ? ((int) (valueMarker.endIndex - valueMarker.startIndex)) : valueTid.length;
        if (length < 0) {
            return IntegerSize.BIG_INTEGER;
        } else if (length < INT_SIZE_IN_BYTES) {
            return IntegerSize.INT;
        } else if (length == INT_SIZE_IN_BYTES) {
            return (minorVersion != 0 || classifyInteger_1_0()) ? IntegerSize.INT : IntegerSize.LONG;
        } else if (length < LONG_SIZE_IN_BYTES) {
            return IntegerSize.LONG;
        } else if (length == LONG_SIZE_IN_BYTES) {
            return (minorVersion != 0 || classifyInteger_1_0()) ? IntegerSize.LONG : IntegerSize.BIG_INTEGER;
        }
        return IntegerSize.BIG_INTEGER;
    }

    private void throwDueToInvalidType(IonType type) {
        throw new IllegalStateException(
            String.format("Invalid type. Required %s but found %s.", type, valueTid == null ? null : valueTid.type)
        );
    }

    @Override
    public int byteSize() {
        if (valueTid == null || !IonType.isLob(valueTid.type) || valueTid.isNull) {
            throw new IonException("Reader must be positioned on a blob or clob.");
        }
        prepareScalar();
        return (int) (valueMarker.endIndex - valueMarker.startIndex);
    }

    @Override
    public byte[] newBytes() {
        byte[] bytes = new byte[byteSize()];
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) valueMarker.startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public int getBytes(byte[] bytes, int offset, int len) {
        int length = Math.min(len, byteSize() - lobBytesRead);
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) (valueMarker.startIndex + lobBytesRead), bytes, offset, length);
        lobBytesRead += length;
        return length;
    }

    /**
     * Loads the scalar converter with an integer value that fits the Ion int on which the reader is positioned.
     */
    private void prepareToConvertIntValue() {
        if (getIntegerSize() == IntegerSize.BIG_INTEGER) {
            scalarConverter.addValue(bigIntegerValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.bigInteger_value);
        } else {
            scalarConverter.addValue(longValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.long_value);
        }
    }

    @Override
    public BigDecimal bigDecimalValue() {
        BigDecimal value = null;
        if (valueTid.type == IonType.DECIMAL) {
            if (valueTid.isNull) {
                return null;
            }
            prepareScalar();
            peekIndex = valueMarker.startIndex;
            if (peekIndex >= valueMarker.endIndex) {
                value = BigDecimal.ZERO;
            } else {
                value = minorVersion == 0 ? readBigDecimal_1_0() : readBigDecimal_1_1();
            }
        } else if (valueTid.type == IonType.INT) {
            if (valueTid.isNull) {
                return null;
            }
            prepareToConvertIntValue();
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.decimal_value));
            value = scalarConverter.getBigDecimal();
            scalarConverter.clear();
        } else {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        return value;
    }

    @Override
    public Decimal decimalValue() {
        Decimal value = null;
        if (valueTid.type == IonType.DECIMAL) {
            if (valueTid.isNull) {
                return null;
            }
            prepareScalar();
            peekIndex = valueMarker.startIndex;
            if (peekIndex >= valueMarker.endIndex) {
                value = Decimal.ZERO;
            } else {
                value = minorVersion == 0 ? readDecimal_1_0() : readDecimal_1_1();
            }
        } else if (valueTid.type == IonType.INT) {
            if (valueTid.isNull) {
                return null;
            }
            prepareToConvertIntValue();
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.decimal_value));
            value = scalarConverter.getDecimal();
            scalarConverter.clear();
        } else {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        return value;
    }

    @Override
    public long longValue() {
        long value;
        if (valueTid.isNull) {
            throwDueToInvalidType(IonType.INT);
        }
        if (valueTid.type == IonType.INT) {
            if (valueTid.length == 0) {
                return 0;
            }
            prepareScalar();
            value = minorVersion == 0 ? readLong_1_0() : readLong_1_1();
        } else if (valueTid.type == IonType.FLOAT) {
            scalarConverter.addValue(doubleValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else if (valueTid.type == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("longValue() may only be called on non-null values of type int, float, or decimal.");
        }
        return value;
    }

    @Override
    public BigInteger bigIntegerValue() {
        BigInteger value;
        if (valueTid.type == IonType.INT) {
            if (valueTid.isNull) {
                // NOTE: this mimics existing behavior, but should probably be undefined (as, e.g., longValue() is in this
                //  case).
                return null;
            }
            if (valueTid.length == 0) {
                return BigInteger.ZERO;
            }
            prepareScalar();
            value = minorVersion == 0 ? readBigInteger_1_0() : readBigInteger_1_1();
        } else if (valueTid.type == IonType.FLOAT) {
            if (valueTid.isNull) {
                value = null;
            } else {
                scalarConverter.addValue(doubleValue());
                scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
                scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.bigInteger_value));
                value = scalarConverter.getBigInteger();
                scalarConverter.clear();
            }
        } else {
            throw new IllegalStateException("longValue() may only be called on values of type int or float.");
        }
        return value;
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    // IEEE-754 half-precision (s=sign, e=exponent, f=fraction): seee_eeff_ffff_ffff
    private static final int FLOAT_16_SIGN_MASK              = 0b1000_0000_0000_0000;
    private static final int FLOAT_16_EXPONENT_MASK          = 0b0111_1100_0000_0000;
    private static final int FLOAT_16_FRACTION_MASK          = 0b0000_0011_1111_1111;

    // float64 bias: 1023; float16 bias: 15. Shift left to align with the masked exponent bits.
    private static final int FLOAT_16_TO_64_EXPONENT_BIAS_CONVERSION = (1023 - 15) << Integer.numberOfTrailingZeros(FLOAT_16_EXPONENT_MASK);
    // The float16 sign bit has bit index 15; the float64 sign bit has bit index 63.
    private static final int FLOAT_16_TO_64_SIGN_SHIFT = 63 - 15;
    // The 5 float16 exponent bits start at index 10; the 11 float64 exponent bits start at index 52.
    private static final int FLOAT_16_TO_64_EXPONENT_SHIFT = 52 - 10;
    // The most significant float16 fraction bit is at index 9; the most significant float64 fraction bit is at index 51.
    private static final int FLOAT_16_TO_64_FRACTION_SHIFT = 51 - 9;

    /**
     * Reads the next two bytes from the given ByteBuffer as a 16-bit float, returning the value as a Java double.
     * @param byteBuffer a buffer positioned at the first byte of the 16-bit float.
     * @return the value.
     */
    private static double readFloat16(ByteBuffer byteBuffer) {
        int bits = byteBuffer.getShort() & TWO_BYTE_MASK;
        int sign = bits & FLOAT_16_SIGN_MASK;
        int exponent = bits & FLOAT_16_EXPONENT_MASK;
        int fraction = bits & FLOAT_16_FRACTION_MASK;
        if (exponent == 0) {
            if (fraction == 0) {
                return sign == 0 ? -0e0 : 0e0;
            }
            // Denormalized
            throw new UnsupportedOperationException("Support for denormalized half-precision floats not yet added.");
        } else if ((exponent ^ FLOAT_16_EXPONENT_MASK) == 0) {
            if (fraction == 0) {
                return sign == 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
            }
            return Double.NaN;
        }
        return Double.longBitsToDouble(
              ((long) sign << FLOAT_16_TO_64_SIGN_SHIFT)
            | ((long) (exponent + FLOAT_16_TO_64_EXPONENT_BIAS_CONVERSION) << FLOAT_16_TO_64_EXPONENT_SHIFT)
            | ((long) fraction << FLOAT_16_TO_64_FRACTION_SHIFT)
        );
    }

    @Override
    public double doubleValue() {
        double value;
        if (valueTid.isNull) {
            throwDueToInvalidType(IonType.FLOAT);
        }
        if (valueTid.type == IonType.FLOAT) {
            prepareScalar();
            int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
            if (length == 0) {
                return 0.0d;
            }
            ByteBuffer bytes = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
            if (length == FLOAT_16_BYTE_LENGTH) {
                if (minorVersion == 0) {
                    throw new IonException("Ion 1.0 floats may may only have length 0, 4, or 8.");
                }
                value = readFloat16(bytes);
            } else if (length == FLOAT_32_BYTE_LENGTH) {
                value = bytes.getFloat();
            } else {
                // Note: there is no need to check for other lengths here; the type ID byte is validated during next().
                value = bytes.getDouble();
            }
        } else if (valueTid.type == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.double_value));
            value = scalarConverter.getDouble();
            scalarConverter.clear();
        } else if (valueTid.type == IonType.INT) {
            prepareToConvertIntValue();
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.double_value));
            value = scalarConverter.getDouble();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("doubleValue() may only be called on non-null values of type float or decimal.");
        }
        return value;
    }

    @Override
    public Timestamp timestampValue() {
        if (valueTid == null || IonType.TIMESTAMP != valueTid.type) {
            throwDueToInvalidType(IonType.TIMESTAMP);
        }
        if (valueTid.isNull) {
            return null;
        }
        prepareScalar();
        peekIndex = valueMarker.startIndex;
        return minorVersion == 0 ? readTimestamp_1_0() : readTimestamp_1_1();
    }

    @Override
    public Date dateValue() {
        Timestamp timestamp = timestampValue();
        if (timestamp == null) {
            return null;
        }
        return timestamp.dateValue();
    }

    @Override
    public boolean booleanValue() {
        if (valueTid == null || IonType.BOOL != valueTid.type || valueTid.isNull) {
            throwDueToInvalidType(IonType.BOOL);
        }
        prepareScalar();
        return minorVersion == 0 ? readBoolean_1_0() : readBoolean_1_1();
    }

    @Override
    public String stringValue() {
        if (valueTid == null || IonType.STRING != valueTid.type) {
            throwDueToInvalidType(IonType.STRING);
        }
        if (valueTid.isNull) {
            return null;
        }
        prepareScalar();
        ByteBuffer utf8InputBuffer = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
        return utf8Decoder.decode(utf8InputBuffer, (int) (valueMarker.endIndex - valueMarker.startIndex));
    }

    @Override
    public int symbolValueId() {
        if (valueTid == null || IonType.SYMBOL != valueTid.type) {
            throwDueToInvalidType(IonType.SYMBOL);
        }
        if (valueTid.isNull) {
            return -1;
        }
        prepareScalar();
        return (int) readUInt(valueMarker.startIndex, valueMarker.endIndex);
    }

    /**
     * Gets the annotation symbol IDs for the current value, reading them from the buffer first if necessary.
     * @return the annotation symbol IDs, or an empty list if the current value is not annotated.
     */
    IntList getAnnotationSidList() {
        annotationSids.clear();
        long savedPeekIndex = peekIndex;
        peekIndex = annotationSequenceMarker.startIndex;
        if (minorVersion == 0) {
            while (peekIndex < annotationSequenceMarker.endIndex) {
                annotationSids.add(readVarUInt_1_0());
            }
        } else {
            while (peekIndex < annotationSequenceMarker.endIndex) {
                annotationSids.add((int) readFlexUInt_1_1());
            }
        }
        peekIndex = savedPeekIndex;
        return annotationSids;
    }

    @Override
    public int[] getAnnotationIds() {
        getAnnotationSidList();
        int[] annotationArray = new int[annotationSids.size()];
        for (int i = 0; i < annotationArray.length; i++) {
            annotationArray[i] = annotationSids.get(i);
        }
        return annotationArray;
    }

    @Override
    public int getFieldId() {
        return fieldSid;
    }

    @Override
    public boolean isInStruct() {
        return parent != null && parent.typeId.type == IonType.STRUCT;
    }

    @Override
    public IonType getType() {
        return valueTid == null ? null : valueTid.type;
    }

    @Override
    public int getDepth() {
        return containerIndex + 1;
    }

    @Override
    public void close() {
        utf8Decoder.close();
        super.close();
    }
}
