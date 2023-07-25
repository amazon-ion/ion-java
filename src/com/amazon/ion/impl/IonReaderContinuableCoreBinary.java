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

/**
 * An IonCursor capable of raw parsing of binary Ion streams.
 */
class IonReaderContinuableCoreBinary extends IonCursorBinary implements IonReaderContinuableCore {

    // Isolates the highest bit in a byte.
    private static final int HIGHEST_BIT_BITMASK = 0x80;

    // Isolates the lowest seven bits in a byte.
    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;

    private static final int SINGLE_BYTE_MASK = 0xFF;

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

    // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MIN_LONG = 0x80;

    // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MAX_LONG = 0x7F;

    // The second-most significant bit in the most significant byte of a VarInt is the sign.
    private static final int VAR_INT_SIGN_BITMASK = 0x40;

    // 32-bit floats must declare length 4.
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
     * Copy the requested number of bytes from the buffer into a scratch buffer of exactly the requested length.
     * @param startIndex the start index from which to copy.
     * @param length the number of bytes to copy.
     * @return the scratch byte array.
     */
    private byte[] copyBytesToScratch(long startIndex, int length) {
        // Note: using reusable scratch buffers makes reading ints and decimals 1-5% faster and causes much less
        // GC churn.
        byte[] bytes = null;
        if (length < scratchForSize.length) {
            bytes = scratchForSize[length];
        }
        if (bytes == null) {
            bytes = new byte[length];
        }
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
            currentByte = buffer[(int)(peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    /**
     * Reads a 2+ byte VarUInt, given the first byte. When called, `peekIndex` must point at the second byte in the
     * VarUInt. When this method returns, `peekIndex` will point at the first byte that follows the VarUInt.
     * NOTE: the VarUInt must fit in an `int`.
     * @param currentByte the first byte in the VarUInt.
     * @return the value.
     */
    int readVarUInt_1_0(byte currentByte) {
        int result = currentByte & LOWER_SEVEN_BITS_BITMASK;
        do {
            // Note: if the varUInt  is malformed such that it extends beyond the declared length of the value *and*
            // beyond the end of the buffer, this will result in IndexOutOfBoundsException because only the declared
            // value length has been filled. Preventing this is simple: if (peekIndex >= valueMarker.endIndex) throw
            // new IonException(); However, we choose not to perform that check here because it is not worth sacrificing
            // performance in this inner-loop code in order to throw one type of exception over another in case of
            // malformed data.
            currentByte = buffer[(int) (peekIndex++)];
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        } while (currentByte >= 0);
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
            // Note: if the varInt is malformed such that it extends beyond the declared length of the value *and*
            // beyond the end of the buffer, this will result in IndexOutOfBoundsException because only the declared
            // value length has been filled. Preventing this is simple: if (peekIndex >= valueMarker.endIndex) throw
            // new IonException(); However, we choose not to perform that check here because it is not worth sacrificing
            // performance in this inner-loop code in order to throw one type of exception over another in case of
            // malformed data.
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
        int month = 0;
        int day = 0;
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
     * Determines whether the 8-byte integer starting at `valueMarker.startIndex` and ending at `valueMarker.endIndex`
     * fits in a long, or requires a BigInteger.
     * @return either `IntegerSize.LONG` or `IntegerSize.BIG_INTEGER`.
     */
    private IntegerSize classifyEightByteInt_1_0() {
        if (valueTid.isNegativeInt) {
            // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
            int firstByte = buffer[(int)(valueMarker.startIndex)] & SINGLE_BYTE_MASK;
            if (firstByte < MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                return IntegerSize.LONG;
            } else if (firstByte > MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                return IntegerSize.BIG_INTEGER;
            }
            for (long i = valueMarker.startIndex + 1; i < valueMarker.endIndex; i++) {
                if (0x00 != buffer[(int)(i)]) {
                    return IntegerSize.BIG_INTEGER;
                }
            }
        } else {
            // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
            if ((buffer[(int)(valueMarker.startIndex)] & SINGLE_BYTE_MASK) > MOST_SIGNIFICANT_BYTE_OF_MAX_LONG) {
                return IntegerSize.BIG_INTEGER;
            }
        }
        return IntegerSize.LONG;
    }


    int readVarUInt_1_1() {
        throw new UnsupportedOperationException();
    }

    private int readVarSym_1_1(Marker marker) {
        throw new UnsupportedOperationException();
    }

    private BigDecimal readBigDecimal_1_1() {
        throw new UnsupportedOperationException();
    }

    private Decimal readDecimal_1_1() {
        throw new UnsupportedOperationException();
    }

    private long readLong_1_1() {
        throw new UnsupportedOperationException();
    }

    private BigInteger readBigInteger_1_1() {
        throw new UnsupportedOperationException();
    }

    private Timestamp readTimestamp_1_1() {
        throw new UnsupportedOperationException();
    }

    private boolean readBoolean_1_1() {
        throw new UnsupportedOperationException();
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
     * Reads a UInt.
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

    @Override
    public boolean isNullValue() {
        return valueTid != null && valueTid.isNull;
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (valueTid.type != IonType.INT || valueTid.isNull) {
            return null;
        }
        if (valueTid.length < 0) {
            return IntegerSize.BIG_INTEGER;
        } else if (valueTid.length < INT_SIZE_IN_BYTES) {
            // Note: this is conservative. Most integers of size 4 also fit in an int, but since exactly the
            // same parsing code is used for ints and longs, there is no point wasting the time to determine the
            // smallest possible type.
            return IntegerSize.INT;
        } else if (valueTid.length < LONG_SIZE_IN_BYTES) {
            return IntegerSize.LONG;
        } else if (valueTid.length == LONG_SIZE_IN_BYTES) {
            // Because creating BigIntegers is so expensive, it is worth it to look ahead and determine exactly
            // which 8-byte integers can fit in a long.
            return minorVersion == 0 ? classifyEightByteInt_1_0() : IntegerSize.LONG;
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

    @Override
    public BigDecimal bigDecimalValue() {
        if (valueTid == null || IonType.DECIMAL != valueTid.type) {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        if (valueTid.isNull) {
            return null;
        }
        peekIndex = valueMarker.startIndex;
        if (peekIndex >= valueMarker.endIndex) {
            return BigDecimal.ZERO;
        }
        return minorVersion == 0 ? readBigDecimal_1_0() : readBigDecimal_1_1();
    }

    @Override
    public Decimal decimalValue() {
        if (valueTid == null || IonType.DECIMAL != valueTid.type) {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        if (valueTid.isNull) {
            return null;
        }
        peekIndex = valueMarker.startIndex;
        if (peekIndex >= valueMarker.endIndex) {
            return Decimal.ZERO;
        }
        return minorVersion == 0 ? readDecimal_1_0() : readDecimal_1_1();
    }

    @Override
    public long longValue() {
        long value;
        if (valueTid.type == IonType.INT && !valueTid.isNull) {
            if (valueTid.length == 0) {
                return 0;
            }
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
            throw new IllegalStateException("longValue() may only be called on values of type int, float, or decimal.");
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

    @Override
    public double doubleValue() {
        double value;
        if (valueTid.type == IonType.FLOAT && !valueTid.isNull) {
            int length = (int) (valueMarker.endIndex - valueMarker.startIndex);
            if (length == 0) {
                return 0.0d;
            }
            ByteBuffer bytes = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
            if (length == FLOAT_32_BYTE_LENGTH) {
                value = bytes.getFloat();
            } else {
                // Note: there is no need to check for other lengths here; the type ID byte is validated during next().
                value = bytes.getDouble();
            }
        }  else if (valueTid.type == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.double_value));
            value = scalarConverter.getDouble();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("doubleValue() may only be called on values of type float or decimal.");
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
                annotationSids.add(readVarUInt_1_1());
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
