// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.MacroAwareIonReader;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion._private.SuppressFBWarnings;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.impl.bin.OpCodes;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.EExpressionArgsReader;
import com.amazon.ion.impl.macro.IonReaderFromReaderAdapter;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroCompiler;
import com.amazon.ion.impl.macro.MacroTable;
import com.amazon.ion.impl.macro.MutableMacroTable;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterContinuable;
import com.amazon.ion.impl.macro.MacroEvaluator;
import com.amazon.ion.impl.macro.MacroEvaluatorAsIonReader;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.SystemMacro;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.SystemSymbols.DEFAULT_MODULE;
import static com.amazon.ion.impl.IonReaderContinuableApplicationBinary.SYMBOLS_LIST_INITIAL_CAPACITY;
import static com.amazon.ion.impl.IonTypeID.SYSTEM_SYMBOL_VALUE;
import static com.amazon.ion.impl.bin.Ion_1_1_Constants.*;

/**
 * An IonCursor capable of raw parsing of binary Ion streams.
 */
class IonReaderContinuableCoreBinary extends IonCursorBinary implements IonReaderContinuableCore, MacroAwareIonReader {

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

    // The core MacroEvaluator that this core reader delegates to when evaluating a macro invocation.
    private final MacroEvaluator macroEvaluator = new MacroEvaluator();

    // The IonReader-like MacroEvaluator that this core reader delegates to when evaluating a macro invocation.
    protected MacroEvaluatorAsIonReader macroEvaluatorIonReader = new MacroEvaluatorAsIonReader(macroEvaluator);

    // The encoding context (macro table) that is currently active.
    private EncodingContext encodingContext = EncodingContext.getDefault();

    // Adapts this reader for use in code that supports multiple reader types.
    private final ReaderAdapter readerAdapter = new ReaderAdapterContinuable(this);

    // Adapts this reader for use in code that supports IonReader.
    private final IonReader asIonReader = new IonReaderFromReaderAdapter(readerAdapter);

    // Reads encoding directives from the stream.
    private final EncodingDirectiveReader encodingDirectiveReader = new EncodingDirectiveReader();

    // Reads macro invocation arguments as expressions and feeds them to the MacroEvaluator.
    private final EExpressionArgsReader expressionArgsReader = new BinaryEExpressionArgsReader();

    // The text representations of the symbol table that is currently in scope, indexed by symbol ID. If the element at
    // a particular index is null, that symbol has unknown text.
    protected String[] symbols = new String[SYMBOLS_LIST_INITIAL_CAPACITY];

    // The maximum offset into the 'symbols' array that points to a valid local symbol.
    protected int localSymbolMaxOffset = -1;

    // The maximum offset into the macro table that points to a valid local macro.
    private int localMacroMaxOffset = -1;

    // Indicates whether the reader is currently evaluating an e-expression.
    protected boolean isEvaluatingEExpression = false;

    // The writer that will perform a macro-aware transcode, if requested.
    private MacroAwareIonWriter macroAwareTranscoder = null;

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
     * Reads a 2+ byte VarUInt, given the first byte. When called, `peekIndex` must point at the second byte in the
     * VarUInt. When this method returns, `peekIndex` will point at the first byte that follows the VarUInt.
     * NOTE: the VarUInt must fit in an `int`.
     * @param currentByte the first byte in the VarUInt.
     * @return the value.
     */
    int readVarUInt_1_0(byte currentByte) {
        int result = currentByte & LOWER_SEVEN_BITS_BITMASK;
        do {
            if (peekIndex >= limit) {
                throw new IonException("Malformed data: declared length exceeds the number of bytes remaining in the stream.");
            }
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
     * Reads a 3+ byte FlexUInt into a long. After this method returns, `peekIndex` points to the first byte after the
     * end of the FlexUInt.
     * @param firstByte the first byte of the FlexUInt.
     * @return the value.
     */
    private long readLargeFlexUInt_1_1(int firstByte) {
        byte length = 0;
        int bitShift = 0;
        if (firstByte == 0) {
            length = 7; // Don't include the skipped zero byte.
            bitShift = -7;
            firstByte = buffer[(int) peekIndex++] & SINGLE_BYTE_MASK;
            if (firstByte == 0) {
                throw new IonException("Flex subfield exceeds the length of a long.");
            }
        }
        length += (byte) (Integer.numberOfTrailingZeros(firstByte) + 1);
        bitShift += length;
        long result = firstByte >>> bitShift;
        for (byte i = 1; i < length; i++) {
            result |= ((long) (buffer[(int) (peekIndex++)] & SINGLE_BYTE_MASK) << (8 * i - bitShift));
        }
        return result;
    }

    /**
     * Reads a FlexUInt into a long. After this method returns, `peekIndex` points to the first byte after the end of
     * the FlexUInt.
     * @return the value.
     */
    long readFlexUInt_1_1() {
        // Up-cast to int, ensuring the most significant bit in the byte is not treated as the sign.
        int currentByte = buffer[(int)(peekIndex++)] & SINGLE_BYTE_MASK;
        if ((currentByte & 1) == 1) {
            // Single byte; shift out the continuation bit.
            return currentByte >>> 1;
        }
        if ((currentByte & 2) != 0) {
            // Two bytes; upcast the second byte to int, ensuring the most significant bit is not treated as the sign.
            // Make room for the six value bits in the first byte. Or with those six value bits after shifting out the
            // two continuation bits.
            return ((buffer[(int) peekIndex++] & SINGLE_BYTE_MASK) << 6 ) | (currentByte >>> 2);
        }
        return readLargeFlexUInt_1_1(currentByte);
    }

    /**
     * Reads a 3+ byte FlexInt into a long. After this method returns, `peekIndex` points to the first byte after the
     * end of the FlexInt.
     * @return the value.
     */
    long readLargeFlexInt_1_1(int firstByte) {
        firstByte &= SINGLE_BYTE_MASK;
        // FlexInts are essentially just FlexUInts that interpret the most significant bit as a sign that needs to be
        // extended.
        long result = readLargeFlexUInt_1_1(firstByte);
        if (buffer[(int) peekIndex - 1] < 0) {
            // Sign extension.
            result |= ~(-1L >>> Long.numberOfLeadingZeros(result));
        }
        return result;
    }

    /**
     * Reads a FlexInt into a long. After this method returns, `peekIndex` points to the first byte after the end of
     * the FlexInt.
     * @return the value.
     */
    long readFlexInt_1_1() {
        // The following up-cast to int performs sign extension, if applicable.
        int currentByte = buffer[(int)(peekIndex++)];
        if ((currentByte & 1) == 1) {
            // Single byte; shift out the continuation bit while preserving the sign.
            return currentByte >> 1;
        }
        if ((currentByte & 2) != 0) {
            // Two bytes; up-cast the second byte to int, thereby performing sign extension. Make room for the six
            // value bits in the first byte. Or with those six value bits after shifting out the two continuation bits.
            return buffer[(int) peekIndex++] << 6 | ((currentByte & SINGLE_BYTE_MASK) >>> 2);
        }
        return readLargeFlexInt_1_1(currentByte);
    }

    /**
     * Reads a FlexSym. After this method returns, `peekIndex` points to the first byte after the end of the FlexSym.
     * When the FlexSym contains inline text, the given Marker's start and end indices are populated with the start and
     * end of the UTF-8 byte sequence, and this method returns -1. When the FlexSym contains a symbol ID, the given
     * Marker's endIndex is set to the symbol ID value and its startIndex is not set. When this FlexSym wraps a
     * delimited end marker, neither the Marker's startIndex nor its endIndex is set.
     * @param markerToSet the marker to populate.
     * @return the symbol ID value if one was present, otherwise -1.
     */
    private long readFlexSym_1_1(Marker markerToSet) {
        // TODO find a factoring that reduces duplication with IonCursorBinary, taking into account performance.
        long result = readFlexInt_1_1();
        if (result == 0) {
            int nextByte = buffer[(int)(peekIndex++)];
            // We pretend $0 is a system symbol to reduce the number of branches here.
            if (nextByte >= FLEX_SYM_SYSTEM_SYMBOL_OFFSET || nextByte <= (byte) (FLEX_SYM_SYSTEM_SYMBOL_OFFSET + Byte.MAX_VALUE)) {
                markerToSet.typeId = SYSTEM_SYMBOL_VALUE;
                markerToSet.startIndex = -1;
                markerToSet.endIndex = (byte)(nextByte - FLEX_SYM_SYSTEM_SYMBOL_OFFSET);
            } else if (nextByte != OpCodes.DELIMITED_END_MARKER) {
                throw new IonException("FlexSym 0 may only precede symbol zero, system symbol, or delimited end.");
            }
            return -1;
        } else if (result < 0) {
            markerToSet.startIndex = peekIndex;
            markerToSet.endIndex = peekIndex - result;
            peekIndex = markerToSet.endIndex;
            return -1;
        } else {
            markerToSet.endIndex = result;
        }
        return result;
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
     * Reads a FixedInt or FixedUInt as a BigInteger, first copying the value into scratch space and converting it to
     * its equivalent big-endian two's complement representation. If the provided length is longer than the actual
     * length of the value, the most significant byte in the two's complement representation will be zero, maintaining
     * a positive sign.
     * @param length the number of bytes remaining in the FixedInt or FixedUInt representation.
     * @return a new BigInteger that represents the value.
     */
    private BigInteger readLargeFixedIntOrFixedUIntAsBigInteger(int length) {
        // FixedInt is a little-endian two's complement representation. Simply reverse the bytes.
        byte[] bytes = getScratchForSize(length);
        // Clear the most significant byte in case the scratch space is padded to accommodate an unsigned value with
        // its highest bit set.
        bytes[0] = 0;
        int copyIndex = bytes.length;
        for (long i = peekIndex; i < valueMarker.endIndex; i++) {
            bytes[--copyIndex] = buffer[(int) i];
        }
        peekIndex = valueMarker.endIndex;
        return new BigInteger(bytes);
    }

    /**
     * Reads a FixedUInt value into a BigInteger.
     * @return the value.
     */
    private BigInteger readFixedUIntAsBigInteger_1_1(int length) {
        if (buffer[(int) valueMarker.endIndex - 1] < 0) {
            // The most-significant bit is set; pad the length by one byte so that the value remains unsigned.
            length += 1;
        }
        return readLargeFixedIntOrFixedUIntAsBigInteger(length);
    }

    /**
     * Reads a FlexUInt or FlexInt value into a BigInteger.
     * @param length the byte length of the encoded FlexUInt or FlexInt to read.
     * @return the value.
     */
    private BigInteger readLargeFlexIntOrFlexUIntAsBigInteger(int length) {
        int bitShift = length;
        int maskForLength = (SINGLE_BYTE_MASK >>> (8 - bitShift));
        int numberOfLeadingZeroBytes = 0;
        // First count the leading zeroes and calculate the number of bits that need to be shifted out of each
        // encoded byte.
        for (long i = peekIndex; i < valueMarker.endIndex; i++) {
            int b = buffer[(int) i] & SINGLE_BYTE_MASK;
            if (b == 0) {
                bitShift -= 8;
                numberOfLeadingZeroBytes++;
                maskForLength = (SINGLE_BYTE_MASK >>> (8 - bitShift));
                continue; // Skip over any bytes that contain only continuation bits.
            }
            break;
        }
        // FlexInt and FlexUInt are little-endian. Reverse the bytes and shift out the continuation bits.
        byte[] bytes = getScratchForSize(length - numberOfLeadingZeroBytes);
        int copyIndex = bytes.length;
        for (long i = peekIndex + numberOfLeadingZeroBytes; i < valueMarker.endIndex; i++) {
            int b = buffer[(int) i] & SINGLE_BYTE_MASK;
            if (copyIndex < bytes.length) {
                bytes[copyIndex] |= (byte) ((b & maskForLength) << (8 - bitShift));
            }
            if (--copyIndex == 0 && !taglessType.isUnsigned) {
                bytes[copyIndex] = (byte) ((byte) b >> bitShift); // Sign extend most significant byte.
            } else {
                bytes[copyIndex] = (byte) (b >>> bitShift);
            }
        }
        peekIndex = valueMarker.endIndex;
        return new BigInteger(bytes);
    }

    /**
     * Reads a tagless int value into a BigInteger.
     * @return the value.
     */
    private BigInteger readTaglessIntAsBigInteger_1_1() {
        BigInteger value;
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (valueTid.variableLength) {
            value = readLargeFlexIntOrFlexUIntAsBigInteger(length);
        } else if (length < LONG_SIZE_IN_BYTES || !taglessType.isUnsigned) {
            // Note: all fixed-width tagless signed ints fit in a Java long.
            value = BigInteger.valueOf(readTaglessInt_1_1());
        } else {
            value = readFixedUIntAsBigInteger_1_1(length);
        }
        return value;
    }

    /**
     * Reads a FixedInt value into a BigInteger.
     * @return the value.
     */
     private BigInteger readFixedIntAsBigInteger_1_1() {
         BigInteger value;
         int length = (int) (valueMarker.endIndex - peekIndex);
         if (length <= LONG_SIZE_IN_BYTES) {
             value = BigInteger.valueOf(readFixedInt_1_1());
         } else {
             value = readLargeFixedIntOrFixedUIntAsBigInteger(length);
         }
         return value;
     }

    /**
     * Reads into a BigDecimal the decimal value that begins at `peekIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private BigDecimal readBigDecimal_1_1() {
        int scale = (int) -readFlexInt_1_1();
        BigDecimal value;
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (length <= LONG_SIZE_IN_BYTES) {
            // No need to allocate a BigInteger to hold the coefficient.
            value = BigDecimal.valueOf(readFixedInt_1_1(), scale);
        } else {
            // The coefficient may overflow a long, so a BigInteger is required.
            value = new BigDecimal(readLargeFixedIntOrFixedUIntAsBigInteger(length), scale);
        }
        return value;
    }

    /**
     * Reads into a Decimal the decimal value that begins at `peekIndex` and ends at `valueMarker.endIndex`.
     * @return the value.
     */
    private Decimal readDecimal_1_1() {
        int scale = (int) -readFlexInt_1_1();
        BigInteger coefficient;
        if (valueMarker.endIndex > peekIndex) {
            // NOTE: there is a BigDecimal.valueOf(long unscaledValue, int scale) factory method that avoids allocating
            // a BigInteger for coefficients that fit in a long. See its use in readBigDecimal() above. Unfortunately,
            // it is not possible to use this for Decimal because the necessary BigDecimal constructor is
            // package-private. If a compatible BigDecimal constructor is added in a future JDK revision, a
            // corresponding factory method should be added to Decimal to enable this optimization.
            coefficient = readFixedIntAsBigInteger_1_1();
            if (coefficient.signum() == 0) {
                return Decimal.negativeZero(scale);
            }
        }
        else {
            coefficient = BigInteger.ZERO;
        }
        return Decimal.valueOf(coefficient, scale);
    }

    /**
     * Reads the tagless int bounded by 'valueMarker` into a long.
     * @return the value.
     */
    private long readTaglessInt_1_1() {
        // TODO performance: the fixed width types all correspond to Java primitives and could therefore be read
        //  using ByteBuffer, possibly more quickly than using the following methods, especially if several in a row
        //  can be read without requiring the cursor's state to be modified before each one.
        if (taglessType.isUnsigned) {
            if (taglessType == TaglessEncoding.FLEX_UINT) {
                return readFlexUInt_1_1();
            }
            return readFixedUInt_1_1(valueMarker.startIndex, valueMarker.endIndex);
        }
        if (taglessType == TaglessEncoding.FLEX_INT) {
            return readFlexInt_1_1();
        }
        return readFixedInt_1_1();
    }

    /**
     * Reads the FixedInt bounded by `valueMarker` into a `long`.
     * @return the value.
     */
    private long readLong_1_1() {
        peekIndex = valueMarker.startIndex;
        if (taglessType != null) {
            return readTaglessInt_1_1();
        }
        return readFixedInt_1_1();
    }

    /**
     * Reads the FixedInt bounded by `valueMarker` into a BigInteger.
     * @return the value.
     */
    private BigInteger readBigInteger_1_1() {
        peekIndex = valueMarker.startIndex;
        if (taglessType != null) {
            return readTaglessIntAsBigInteger_1_1();
        }
        return readFixedIntAsBigInteger_1_1();
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
        int length = (int) (valueMarker.endIndex - peekIndex);
        if (length >= LONG_SIZE_IN_BYTES) {
            value = new BigDecimal(readFixedUIntAsBigInteger_1_1(length), scale);
        } else if (length > 0) {
            value = BigDecimal.valueOf(readFixedUInt_1_1(peekIndex, valueMarker.endIndex), scale);
        } else {
            value = BigDecimal.valueOf(0, scale);
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
    @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
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
    @SuppressFBWarnings("SF_SWITCH_FALLTHROUGH")
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
        // Boolean 'true' is 0x6E; 'false' is 0x6F.
        return valueTid.lowerNibble == 0xE;
    }

    /**
     * Determines whether the bytes between [start, end) in 'buffer' match the target bytes.
     * @param target the target bytes.
     * @param buffer the bytes to match.
     * @param start index of the first byte to match.
     * @param end index of the first byte after the last byte to match.
     * @return true if the bytes match; otherwise, false.
     */
    static boolean bytesMatch(byte[] target, byte[] buffer, int start, int end) {
        // TODO if this ends up on a critical performance path, see if it's faster to copy the bytes into a
        //  pre-allocated buffer and then perform a comparison. It's possible that a combination of System.arraycopy()
        //  and Arrays.equals(byte[], byte[]) is faster because it can be more easily optimized with native code by the
        //  JVM—both are annotated with @HotSpotIntrinsicCandidate.
        int length = end - start;
        if (length != target.length) {
            return false;
        }
        for (int i = 0; i < target.length; i++) {
            if (target[i] != buffer[start + i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if current value has a sequence of annotations that begins with `$ion`; otherwise, false.
     */
    boolean startsWithIonAnnotation() {
        if (minorVersion > 0) {
            Marker marker = annotationTokenMarkers.get(0);
            return matchesSystemSymbol_1_1(marker, SystemSymbols_1_1.ION);
        }
        return false;
    }

    @Override
    public String getSymbol(int sid) {
        // Only symbol IDs declared in Ion 1.1 encoding directives (not Ion 1.0 symbol tables) are resolved by the
        // core reader. In Ion 1.0, 'symbols' is never populated by the core reader.
        if (sid > 0 && sid - 1 <= localSymbolMaxOffset) {
            return symbols[sid - 1];
        }
        return null;
    }

    /**
     * Returns true if the symbol at `marker`...
     * <p> * is a system symbol with the same ID as the expected System Symbol
     * <p> * is an inline symbol with the same utf8 bytes as the expected System Symbol
     * <p> * is a user symbol that maps to the same text as the expected System Symbol
     * <p>
     */
    boolean matchesSystemSymbol_1_1(Marker marker, SystemSymbols_1_1 systemSymbol) {
        if (marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
            return systemSymbol.getText().equals(getSystemSymbolToken(marker).getText());
        } else if (marker.startIndex < 0) {
            // This is a local symbol whose ID is stored in marker.endIndex.
            return systemSymbol.getText().equals(getSymbol((int) marker.endIndex));
        } else {
            // This is an inline symbol with UTF-8 bytes bounded by the marker.
            return bytesMatch(systemSymbol.getUtf8Bytes(), buffer, (int) marker.startIndex, (int) marker.endIndex);
        }
    }

    /**
     * @return true if the reader is positioned on an encoding directive; otherwise, false.
     */
    private boolean isPositionedOnEncodingDirective() {
        return event == Event.START_CONTAINER
            && hasAnnotations
            && valueTid.type == IonType.SEXP
            && parent == null
            && startsWithIonAnnotation();
    }

    /**
     * @return true if the macro evaluator is positioned on an encoding directive; otherwise, false.
     */
    private boolean isPositionedOnEvaluatedEncodingDirective() {
        if (macroEvaluatorIonReader.getType() != IonType.SEXP) {
            return false;
        }
        Iterator<String> annotations = macroEvaluatorIonReader.iterateTypeAnnotations();
        return annotations.hasNext()
            && annotations.next().equals(SystemSymbols_1_1.ION.getText());
    }

    /**
     * Grows the `symbols` array to the next power of 2 that will fit the current need.
     */
    protected void growSymbolsArray(int shortfall) {
        int newSize = nextPowerOfTwo(symbols.length + shortfall);
        String[] resized = new String[newSize];
        System.arraycopy(symbols, 0, resized, 0, localSymbolMaxOffset + 1);
        symbols = resized;
    }

    /**
     * Reset the local symbol table to the system symbol table.
     */
    protected void resetSymbolTable() {
        // The following line is not required for correctness, but it frees the references to the old symbols,
        // potentially allowing them to be garbage collected.
        Arrays.fill(symbols, 0, localSymbolMaxOffset + 1, null);
        localSymbolMaxOffset = -1;
    }

    /**
     * Reset the list of imported shared symbol tables.
     */
    protected void resetImports(int major, int minor) {
        // The core reader does not currently handle imports, though we may find this necessary as we add
        // support for shared modules.
    }

    /**
     * Installs the given symbols at the end of the `symbols` array.
     * @param newSymbols the symbols to install.
     */
    protected void installSymbols(List<String> newSymbols) {
        if (newSymbols != null && !newSymbols.isEmpty()) {
            int numberOfNewSymbols = newSymbols.size();
            int numberOfAvailableSlots = symbols.length - (localSymbolMaxOffset + 1);
            int shortfall = numberOfNewSymbols - numberOfAvailableSlots;
            if (shortfall > 0) {
                growSymbolsArray(shortfall);
            }
            int i = localSymbolMaxOffset;
            for (String newSymbol : newSymbols) {
                symbols[++i] = newSymbol;
            }
            localSymbolMaxOffset += newSymbols.size();
        }
    }

    @Override
    EncodingContext getEncodingContext() {
        return encodingContext;
    }

    /**
     * Reads encoding directives from the stream. Capable of resuming if not enough data is currently available to
     * complete the encoding directive.
     */
    private class EncodingDirectiveReader {

        boolean isSymbolTableAppend = false;
        boolean isMacroTableAppend = false;
        List<String> newSymbols = new ArrayList<>(8);
        Map<MacroRef, Macro> newMacros = new LinkedHashMap<>();
        MacroCompiler macroCompiler = new MacroCompiler(this::resolveMacro, readerAdapter);

        boolean isSymbolTableAlreadyClassified = false;
        boolean isMacroTableAlreadyClassified = false;

        private Macro resolveMacro(MacroRef macroRef) {
            Macro newMacro = newMacros.get(macroRef);
            if (newMacro == null) {
                newMacro = encodingContext.getMacroTable().get(macroRef);
            }
            return newMacro;
        }

        private boolean valueUnavailable() {
            if (isEvaluatingEExpression) {
                return false;
            }
            Event event = fillValue();
            return event == Event.NEEDS_DATA || event == Event.NEEDS_INSTRUCTION;
        }

        private void classifyDirective() {
            errorIf(getEncodingType() != IonType.SYMBOL, "Ion encoding directives must start with a directive keyword.");
            String name = getSymbolText();
            // TODO: Add support for `import` and `encoding` directives
            if (SystemSymbols_1_1.MODULE.getText().equals(name)) {
                state = State.IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME;
            } else if (SystemSymbols_1_1.IMPORT.getText().equals(name)) {
                throw new IonException("'import' directive not yet supported");
            } else if (SystemSymbols_1_1.ENCODING.getText().equals(name)) {
                throw new IonException("'encoding' directive not yet supported");
            } else {
                throw new IonException(String.format("'%s' is not a valid directive keyword", name));
            }
        }

        private void classifySexpWithinModuleDirective() {
            String name = getSymbolText();
            if (SystemSymbols_1_1.SYMBOL_TABLE.getText().equals(name)) {
                state = State.IN_SYMBOL_TABLE_SEXP;
            } else if (SystemSymbols_1_1.MACRO_TABLE.getText().equals(name)) {
                state = State.IN_MACRO_TABLE_SEXP;
            } else {
                // TODO: add support for 'module' and 'import' clauses
                throw new IonException(String.format("'%s' clause not supported in module definition", name));
            }
        }

        /**
         * Classifies a symbol table as either 'set' or 'append'. The caller must ensure the reader is positioned within
         * a symbol table (after the symbol 'symbol_table') before calling. Upon return, the reader will be positioned
         * on a list in the symbol table.
         */
        private void classifySymbolTable() {
            IonType type = getEncodingType();
            if (isSymbolTableAlreadyClassified) {
                if (type != IonType.LIST) { // TODO support module name imports
                    throw new IonException("symbol_table s-expression must contain list(s) of symbols.");
                }
                state = State.ON_SYMBOL_TABLE_LIST;
                return;
            }
            isSymbolTableAlreadyClassified = true;
            if (IonType.isText(type)) {
                if (DEFAULT_MODULE.equals(stringValue()) && !isSymbolTableAppend) {
                    state = State.IN_APPENDED_SYMBOL_TABLE;
                } else {
                    throw new IonException("symbol_table s-expression must begin with either '_' or a list.");
                }
            } else if (type == IonType.LIST) {
                state = State.ON_SYMBOL_TABLE_LIST;
            } else {
                throw new IonException("symbol_table s-expression must begin with either '_' or a list.");
            }
        }

        /**
         * Classifies a macro table as either 'set' or 'append'. The caller must ensure the reader is positioned within
         * a macro table (after the symbol 'macro_table') before calling. Upon return, the reader will be positioned
         * on an s-expression in the macro table.
         */
        private void classifyMacroTable() {
            IonType type = getEncodingType();
            if (isMacroTableAlreadyClassified) {
                if (type != IonType.SEXP) {
                    throw new IonException("macro_table s-expression must contain s-expression(s).");
                }
                state = State.ON_MACRO_SEXP;
                return;
            }
            isMacroTableAlreadyClassified = true;
            if (IonType.isText(type)) {
                if (DEFAULT_MODULE.equals(stringValue()) && !isMacroTableAppend) {
                    state = State.IN_APPENDED_MACRO_TABLE;
                } else {
                    throw new IonException("macro_table s-expression must begin with either '_' or s-expression(s).");
                }
            } else if (type == IonType.SEXP) {
                localMacroMaxOffset = -1;
                state = State.ON_MACRO_SEXP;
            } else {
                throw new IonException("macro_table s-expression must contain s-expression(s).");
            }
        }

        private void stepOutOfSexpWithinEncodingDirective() {
            stepOutOfContainer();
            state = State.IN_MODULE_DIRECTIVE_SEXP_BODY;
        }

        /**
         * Install `newMacros`, initializing a macro evaluator capable of evaluating them.
         */
        private void installMacros() {
            if (!isMacroTableAppend) {
                encodingContext = new EncodingContext(new MutableMacroTable(MacroTable.empty()));
            } else if (!encodingContext.isMutable()) { // we need to append, but can't
                encodingContext = new EncodingContext(new MutableMacroTable(encodingContext.getMacroTable()));
            }

            if (newMacros.isEmpty()) return; // our work is done

            encodingContext.getMacroTable().putAll(newMacros);
        }

        /**
         * Install any new symbols and macros, step out of the encoding directive, and resume reading raw values.
         */
        private void finishEncodingDirective() {
            if (!isSymbolTableAppend) {
                resetSymbolTable();
            }
            installSymbols(newSymbols);
            installMacros();
            stepOutOfContainer();
            state = State.READING_VALUE;
        }

        /**
         * Navigate to the next value at the core level (without interpretation by subclasses).
         * @return the event that conveys the result of the operation.
         */
        private Event coreNextValue() {
            if (isEvaluatingEExpression) {
                evaluateNext();
                return event;
            } else {
                return IonReaderContinuableCoreBinary.super.nextValue();
            }
        }

        /**
         * Utility function to make error cases more concise.
         * @param condition the condition under which an IonException should be thrown
         * @param errorMessage the message to use in the exception
         */
        private void errorIf(boolean condition, String errorMessage) {
            if (condition) {
                throw new IonException(errorMessage);
            }
        }

        /**
         * Read an encoding directive. If the stream ends before the encoding directive finishes, `event` will be
         * `NEEDS_DATA` and this method can be called again when more data is available.
         */
        void readEncodingDirective() {
            Event event;
            while (true) {
                switch (state) {
                    case ON_DIRECTIVE_SEXP:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            return;
                        }
                        state = State.IN_DIRECTIVE_SEXP;
                        break;
                    case IN_DIRECTIVE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        errorIf(event == Event.END_CONTAINER, "invalid Ion directive; missing directive keyword");
                        classifyDirective();
                        break;
                    case IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        errorIf(event == Event.END_CONTAINER, "invalid module definition; missing module name");
                        errorIf(getEncodingType() != IonType.SYMBOL, "invalid module definition; module name must be a symbol");
                        // TODO: Support other module names
                        errorIf(!DEFAULT_MODULE.equals(getSymbolText()), "IonJava currently supports only the default module");
                        state = State.IN_MODULE_DIRECTIVE_SEXP_BODY;
                        break;
                    case IN_MODULE_DIRECTIVE_SEXP_BODY:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishEncodingDirective();
                            return;
                        }
                        if (getEncodingType() != IonType.SEXP) {
                            throw new IonException("module definitions must contain only s-expressions.");
                        }
                        state = State.ON_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case ON_SEXP_IN_MODULE_DIRECTIVE:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            return;
                        }
                        state = State.IN_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case IN_SEXP_IN_MODULE_DIRECTIVE:
                        if (Event.NEEDS_DATA == coreNextValue()) {
                            return;
                        }
                        if (!IonType.isText(getEncodingType())) {
                            throw new IonException("S-expressions within module definitions must begin with a text token.");
                        }
                        state = State.CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE:
                        if (valueUnavailable()) {
                            return;
                        }
                        classifySexpWithinModuleDirective();
                        break;
                    case IN_SYMBOL_TABLE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        classifySymbolTable();
                        break;
                    case IN_APPENDED_SYMBOL_TABLE:
                        event = coreNextValue();
                        if (Event.NEEDS_DATA == event) {
                            return;
                        }
                        isSymbolTableAppend = true;
                        if (Event.END_CONTAINER == event) {
                            // Nothing to append.
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        if (getEncodingType() != IonType.LIST) {
                            throw new IonException("symbol_table s-expression must begin with a list.");
                        }
                        state = State.ON_SYMBOL_TABLE_LIST;
                        break;
                    case ON_SYMBOL_TABLE_LIST:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            return;
                        }
                        state = State.IN_SYMBOL_TABLE_LIST;
                        break;
                    case IN_SYMBOL_TABLE_LIST:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfContainer();
                            state = State.IN_SYMBOL_TABLE_SEXP;
                            break;
                        }
                        if (!IonType.isText(getEncodingType())) {
                            throw new IonException("The symbol_table must contain text.");
                        }
                        state = State.ON_SYMBOL;
                        break;
                    case ON_SYMBOL:
                        if (valueUnavailable()) {
                            return;
                        }
                        newSymbols.add(stringValue());
                        state = State.IN_SYMBOL_TABLE_LIST;
                        break;
                    case IN_MACRO_TABLE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            return;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        classifyMacroTable();
                        break;
                    case IN_APPENDED_MACRO_TABLE:
                        event = coreNextValue();
                        if (Event.NEEDS_DATA == event) {
                            return;
                        }
                        isMacroTableAppend = true;
                        if (event == Event.END_CONTAINER) {
                            // Nothing to append
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        } if (getEncodingType() != IonType.SEXP) {
                            throw new IonException("macro_table s-expression must contain s-expressions.");
                        }
                        state = State.ON_MACRO_SEXP;
                        break;
                    case ON_MACRO_SEXP:
                        if (valueUnavailable()) {
                            return;
                        }
                        state = State.COMPILING_MACRO;
                        Macro newMacro = macroCompiler.compileMacro();
                        newMacros.put(MacroRef.byId(++localMacroMaxOffset), newMacro);
                        String macroName = macroCompiler.getMacroName();
                        if (macroName != null) {
                            newMacros.put(MacroRef.byName(macroName), newMacro);
                        }
                        state = State.IN_MACRO_TABLE_SEXP;
                        break;
                    default:
                        throw new IllegalStateException(state.toString());
                }
            }
        }

        void resetState() {
            isSymbolTableAppend = false;
            isSymbolTableAlreadyClassified = false;
            newSymbols.clear();
            isMacroTableAppend = false;
            isMacroTableAlreadyClassified = false;
            newMacros.clear();
        }
    }

    /**
     * The reader's state. `READING_VALUE` indicates that the reader is reading a raw value; all other states
     * indicate that the reader is in the middle of reading an encoding directive.
     */
    private enum State {
        ON_DIRECTIVE_SEXP,
        IN_DIRECTIVE_SEXP,
        IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME,
        IN_MODULE_DIRECTIVE_SEXP_BODY,
        ON_SEXP_IN_MODULE_DIRECTIVE,
        IN_SEXP_IN_MODULE_DIRECTIVE,
        CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE,
        IN_SYMBOL_TABLE_SEXP,
        IN_APPENDED_SYMBOL_TABLE,
        ON_SYMBOL_TABLE_LIST,
        IN_SYMBOL_TABLE_LIST,
        ON_SYMBOL,
        IN_MACRO_TABLE_SEXP,
        IN_APPENDED_MACRO_TABLE,
        ON_MACRO_SEXP,
        COMPILING_MACRO,
        READING_VALUE,
    }

    // The current state.
    private State state = State.READING_VALUE;

    /**
     * Reads macro invocation arguments as expressions and feeds them to the MacroEvaluator.
     */
    private class BinaryEExpressionArgsReader extends EExpressionArgsReader {

        BinaryEExpressionArgsReader() {
            super (readerAdapter);
        }

        /**
         * Reads a single (non-grouped) expression.
         * @param parameter the parameter.
         */
        private void readSingleExpression(Macro.Parameter parameter) {
            Macro.ParameterEncoding encoding = parameter.getType();
            if (encoding == Macro.ParameterEncoding.Tagged) {
                IonReaderContinuableCoreBinary.super.nextValue();
            } else {
                nextTaglessValue(encoding.taglessEncodingKind);
            }
            if (event == Event.NEEDS_DATA) {
                throw new UnsupportedOperationException("TODO: support continuable parsing of macro arguments.");
            }
            readValueAsExpression(false);
        }

        /**
         * Reads a group expression.
         * @param parameter the parameter.
         */
        private void readGroupExpression(Macro.Parameter parameter, boolean requireSingleton) {
            Macro.ParameterEncoding encoding = parameter.getType();
            if (encoding == Macro.ParameterEncoding.Tagged) {
                enterTaggedArgumentGroup();
            } else {
                enterTaglessArgumentGroup(encoding.taglessEncodingKind);
            }
            if (event == Event.NEEDS_DATA) {
                throw new UnsupportedOperationException("TODO: support continuable parsing of macro arguments.");
            }
            int startIndex = expressions.size();
            expressions.add(Expression.Placeholder.INSTANCE);
            boolean isSingleton = true;
            while (nextGroupedValue() != Event.NEEDS_INSTRUCTION || isMacroInvocation()) {
                readValueAsExpression(false);
                isSingleton = false;
            }
            if (requireSingleton && !isSingleton) {
                throw new IonException(String.format(
                    "Parameter %s with cardinality %s must not contain multiple expressions.",
                    parameter.getVariableName(),
                    parameter.getCardinality().name())
                );
            }
            if (exitArgumentGroup() == Event.NEEDS_DATA) {
                throw new UnsupportedOperationException("TODO: support continuable parsing of macro arguments.");
            }
            expressions.set(startIndex, expressionPool.createExpressionGroup(startIndex, expressions.size()));
        }

        /**
         * Adds an expression that conveys that the parameter was not present (void).
         */
        private void addVoidExpression() {
            int startIndex = expressions.size();
            expressions.add(expressionPool.createExpressionGroup(startIndex, startIndex + 1));
        }

        @Override
        protected void readParameter(Macro.Parameter parameter, long parameterPresence, boolean isTrailing) {
            switch (parameter.getCardinality()) {
                case ZeroOrOne:
                    if (parameterPresence == PresenceBitmap.EXPRESSION) {
                        readSingleExpression(parameter);
                    } else if (parameterPresence == PresenceBitmap.VOID) {
                        addVoidExpression();
                    } else if (parameterPresence == PresenceBitmap.GROUP) {
                        readGroupExpression(parameter, true);
                    } else {
                        throw new IllegalStateException("Unreachable: presence bitmap validated but reserved bits found.");
                    }
                    break;
                case ExactlyOne:
                    // TODO determine if a group with a single element is valid here.
                    readSingleExpression(parameter);
                    break;
                case OneOrMore:
                    if (parameterPresence == PresenceBitmap.EXPRESSION) {
                        readSingleExpression(parameter);
                    } else if (parameterPresence == PresenceBitmap.GROUP) {
                        readGroupExpression(parameter, false);
                    } else {
                        throw new IonException(String.format(
                            "Invalid void argument for non-voidable parameter: %s",
                            parameter.getVariableName())
                        );
                    }
                    break;
                case ZeroOrMore:
                    if (parameterPresence == PresenceBitmap.EXPRESSION) {
                        readSingleExpression(parameter);
                    } else if (parameterPresence == PresenceBitmap.GROUP) {
                        readGroupExpression(parameter, false);
                    } else if (parameterPresence == PresenceBitmap.VOID) {
                        addVoidExpression();
                    } else {
                        throw new IllegalStateException("Unreachable: presence bitmap validated but reserved bits found.");
                    }
                    break;
            }
        }

        @Override
        protected Macro loadMacro() {
            Macro macro;
            long id = getMacroInvocationId();
            if (isSystemInvocation()) {
                macro = SystemMacro.get((int) id);
                if (macro == null) {
                    throw new UnsupportedOperationException("System macro " + id + " not yet supported.");
                }
            } else {
                if (id > Integer.MAX_VALUE) {
                    throw new IonException("Macro addresses larger than 2147483647 are not supported by this implementation.");
                }
                MacroRef address = MacroRef.byId((int) id);
                macro = encodingContext.getMacroTable().get(address);

                if (macro == null) {
                    throw new IonException(String.format("Encountered an unknown macro address: %d.", id));
                }
            }
            return macro;
        }

        @Override
        protected PresenceBitmap loadPresenceBitmapIfNecessary(List<Macro.Parameter> signature) {
            return IonReaderContinuableCoreBinary.this.loadPresenceBitmap(signature);
        }

        @Override
        protected boolean isMacroInvocation() {
            return valueTid != null && valueTid.isMacroInvocation;
        }

        @Override
        protected boolean isContainerAnExpressionGroup() {
            // In binary, expression groups denoted by the AEB, not using container syntax.
            return false;
        }

        @Override
        protected List<SymbolToken> getAnnotations() {
            if (!hasAnnotations) {
                return Collections.emptyList();
            }
            List<SymbolToken> out = new ArrayList<>();
            consumeAnnotationTokens(out::add);
            return out;
        }

        @Override
        protected boolean nextRaw() {
            return IonReaderContinuableCoreBinary.super.nextValue() != Event.END_CONTAINER;
        }

        @Override
        protected void stepInRaw() {
            IonReaderContinuableCoreBinary.super.stepIntoContainer();
        }

        @Override
        protected void stepOutRaw() {
            IonReaderContinuableCoreBinary.super.stepOutOfContainer();
        }

        @Override
        protected void stepIntoEExpression() {
            IonReaderContinuableCoreBinary.super.stepIntoEExpression();
        }

        @Override
        protected void stepOutOfEExpression() {
            validateValueEndIndex(parent.endIndex);
            IonReaderContinuableCoreBinary.super.stepOutOfEExpression();
        }
    }

    /**
     * @return true if current value has a sequence of annotations that begins with `$ion_symbol_table`; otherwise,
     *  false.
     */
    protected boolean startsWithIonSymbolTable() {
        if (minorVersion == 0 && annotationSequenceMarker.startIndex >= 0) {
            long savedPeekIndex = peekIndex;
            peekIndex = annotationSequenceMarker.startIndex;
            int sid = readVarUInt_1_0();
            peekIndex = savedPeekIndex;
            return ION_SYMBOL_TABLE_SID == sid;
        } else if (minorVersion == 1) {
            Marker marker = annotationTokenMarkers.get(0);
            return matchesSystemSymbol_1_1(marker, SystemSymbols_1_1.ION_SYMBOL_TABLE);
        }
        return false;
    }

    /**
     * @return true if the reader is positioned on a symbol table; otherwise, false.
     */
    protected boolean isPositionedOnSymbolTable() {
        return hasAnnotations &&
            getEncodingType() == IonType.STRUCT &&
            startsWithIonSymbolTable();
    }

    /**
     * Consumes the next value (if any) from the MacroEvaluator, setting `event` based on the result.
     * @return true if evaluation of the current invocation has completed; otherwise, false.
     */
    private boolean evaluateNext() {
        IonType type = macroEvaluatorIonReader.next();
        if (type == null) {
            if (macroEvaluatorIonReader.getDepth() == 0) {
                // Evaluation of this macro is complete. Resume reading from the stream.
                isEvaluatingEExpression = false;
                event = Event.NEEDS_INSTRUCTION;
                return true;
            } else {
                event = Event.END_CONTAINER;
            }
        } else {
            if (IonType.isContainer(type)) {
                event = Event.START_CONTAINER;
            } else {
                event = Event.START_SCALAR;
            }
        }
        return false;
    }

    @Override
    public void transcodeAllTo(MacroAwareIonWriter writer) throws IOException {
        prepareTranscodeTo(writer);
        while (transcodeNext());
    }

    @Override
    public void prepareTranscodeTo(MacroAwareIonWriter writer) {
        registerIvmNotificationConsumer((major, minor) -> {
            resetEncodingContext();
            // Which IVM to write is inherent to the writer implementation.
            // We don't have a single implementation that writes both formats.
            writer.startEncodingSegmentWithIonVersionMarker();
        });
        macroAwareTranscoder = writer;
    }

    @Override
    public boolean transcodeNext() throws IOException {
        if (macroAwareTranscoder == null) {
            throw new IllegalArgumentException("prepareTranscodeTo must be called before transcodeNext.");
        }
        // NOTE: this method is structured very similarly to nextValue(). During performance analysis, we should
        // see if the methods can be unified without sacrificing hot path performance. Performance of this method
        // is not considered critical.
        lobBytesRead = 0;
        while (true) {
            if (parent == null || state != State.READING_VALUE) {
                boolean isEncodingDirective = false;
                if (state != State.READING_VALUE && state != State.COMPILING_MACRO) {
                    boolean isEncodingDirectiveFromEExpression = isEvaluatingEExpression;
                    encodingDirectiveReader.readEncodingDirective();
                    if (state != State.READING_VALUE) {
                        throw new IonException("Unexpected EOF when writing encoding-level value.");
                    }
                    // If the encoding directive was expanded from an e-expression, that expression has already been
                    // written. In that case, just make sure the writer is using the new context. Otherwise, also write
                    // the encoding directive.
                    macroAwareTranscoder.startEncodingSegmentWithEncodingDirective(
                        encodingDirectiveReader.newMacros,
                        encodingDirectiveReader.isMacroTableAppend,
                        encodingDirectiveReader.newSymbols,
                        encodingDirectiveReader.isSymbolTableAppend,
                        isEncodingDirectiveFromEExpression
                    );
                    isEncodingDirective = true;
                }
                if (isEvaluatingEExpression) {
                    if (evaluateNext()) {
                        if (isEncodingDirective) {
                            continue;
                        }
                        // This is the end of a top-level macro invocation that expanded to a user value.
                        return true;
                    }
                } else {
                    event = super.nextValue();
                }
                if (minorVersion == 1 && parent == null && isPositionedOnEncodingDirective()) {
                    encodingDirectiveReader.resetState();
                    state = State.ON_DIRECTIVE_SEXP;
                    continue;
                }
            } else if (isEvaluatingEExpression) {
                if (evaluateNext()) {
                    // This is the end of a contained macro invocation; continue iterating through the parent container.
                    continue;
                }
            } else {
                event = super.nextValue();
            }
            if (valueTid != null && valueTid.isMacroInvocation) {
                expressionArgsReader.beginEvaluatingMacroInvocation(macroEvaluator);
                macroEvaluatorIonReader.transcodeArgumentsTo(macroAwareTranscoder);
                isEvaluatingEExpression = true;
                if (evaluateNext()) {
                    // This macro invocation expands to nothing; continue iterating until a user value is found.
                    continue;
                }
                if (parent == null && isPositionedOnEvaluatedEncodingDirective()) {
                    encodingDirectiveReader.resetState();
                    state = State.ON_DIRECTIVE_SEXP;
                    continue;
                }
            }
            if (isEvaluatingEExpression) {
                // EExpressions are not expanded and provided to the writer; only the raw encoding is transferred.
                continue;
            }
            break;
        }
        if (event == Event.NEEDS_DATA || event == Event.END_CONTAINER) {
            return false;
        }
        transcodeValueLiteral();
        return true;
    }

    /**
     * Transcodes a value literal to the macroAwareTranscoder. The caller must ensure that the reader is positioned
     * on a value literal (i.e. a scalar or container value not expanded from an e-expression) before calling this
     * method.
     * @throws IOException if thrown by the writer during transcoding.
     */
    private void transcodeValueLiteral() throws IOException {
        if (parent == null && isPositionedOnSymbolTable()) {
            if (minorVersion > 0) {
                // TODO finalize handling of Ion 1.0-style symbol tables in Ion 1.1: https://github.com/amazon-ion/ion-java/issues/1002
                throw new IonException("Macro-aware transcoding of Ion 1.1 data containing Ion 1.0-style symbol tables not yet supported.");
            }
            // Ion 1.0 symbol tables are transcoded verbatim for now; this may change depending on the resolution to
            // https://github.com/amazon-ion/ion-java/issues/1002.
            macroAwareTranscoder.writeValue(asIonReader);
        } else if (event == Event.START_CONTAINER && !isNullValue()) {
            // Containers need to be transcoded recursively to avoid expanding macro invocations at any depth.
            if (isInStruct()) {
                macroAwareTranscoder.setFieldNameSymbol(getFieldNameSymbol());
            }
            macroAwareTranscoder.setTypeAnnotationSymbols(asIonReader.getTypeAnnotationSymbols());
            macroAwareTranscoder.stepIn(getEncodingType());
            super.stepIntoContainer();
            while (transcodeNext()); // TODO make this iterative.
            super.stepOutOfContainer();
            macroAwareTranscoder.stepOut();
        } else {
            // The reader is now positioned on a scalar literal. Write the value.
            // Note: writeValue will include any field name and/or annotations on the scalar.
            macroAwareTranscoder.writeValue(asIonReader);
        }
    }

    /**
     * @return true if the reader should evaluate user macro invocations; otherwise false. Should be overridden by
     *   subclasses that support user macros.
     */
    protected boolean evaluateUserMacroInvocations() {
        // The core-level reader does not evaluate macro invocations.
        return false;
    }

    @Override
    public Event nextValue() {
        lobBytesRead = 0;
        while (true) {
            if (parent == null || state != State.READING_VALUE) {
                if (state != State.READING_VALUE && state != State.COMPILING_MACRO) {
                    encodingDirectiveReader.readEncodingDirective();
                    if (state != State.READING_VALUE) {
                        event = Event.NEEDS_DATA;
                        break;
                    }
                }
                if (isEvaluatingEExpression) {
                    if (evaluateNext()) {
                        continue;
                    }
                } else {
                    event = super.nextValue();
                }
                if (minorVersion == 1 && parent == null && isPositionedOnEncodingDirective()) {
                    encodingDirectiveReader.resetState();
                    state = State.ON_DIRECTIVE_SEXP;
                    continue;
                }
            } else if (isEvaluatingEExpression) {
                if (evaluateNext()) {
                    continue;
                }
            } else {
                event = super.nextValue();
            }
            if (valueTid != null && valueTid.isMacroInvocation) {
                if (evaluateUserMacroInvocations() || isSystemInvocation()) {
                    expressionArgsReader.beginEvaluatingMacroInvocation(macroEvaluator);
                    isEvaluatingEExpression = true;
                    if (evaluateNext()) {
                        continue;
                    }
                    if (parent == null && isPositionedOnEvaluatedEncodingDirective()) {
                        encodingDirectiveReader.resetState();
                        state = State.ON_DIRECTIVE_SEXP;
                        continue;
                    }
                }
            }
            break;
        }
        return event;
    }

    @Override
    public Event fillValue() {
        if (isEvaluatingEExpression) {
            event = Event.VALUE_READY;
            return event;
        }
        return super.fillValue();
    }

    @Override
    public Event stepIntoContainer() {
        if (isEvaluatingEExpression) {
            macroEvaluatorIonReader.stepIn();
            event = Event.NEEDS_INSTRUCTION;
            return event;
        }
        return super.stepIntoContainer();
    }

    @Override
    public Event stepOutOfContainer() {
        if (isEvaluatingEExpression) {
            if (macroEvaluatorIonReader.getDepth() > 0) {
                // The user has stepped into a container produced by the evaluator. Therefore, this stepOut() call
                // must step out of that evaluated container.
                macroEvaluatorIonReader.stepOut();
                event = Event.NEEDS_INSTRUCTION;
                return event;
            } else {
                // The evaluator is not producing a container value. Therefore, the user intends for this stepOut() call
                // to step out of the parent container of the e-expression being evaluated. This terminates e-expression
                // evaluation.
                isEvaluatingEExpression = false;
            }
        }
        return super.stepOutOfContainer();
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
     * Reads a FixedUInt (little-endian), for the range of bytes given by `startInclusive` and `endExclusive`.
     * @return the value.
     */
    private long readFixedUInt_1_1(long startInclusive, long endExclusive) {
        long result = 0;
        for (int i = (int) startInclusive; i < endExclusive; i++) {
            result |= ((long) (buffer[i] & SINGLE_BYTE_MASK) << ((i - startInclusive) * VALUE_BITS_PER_UINT_BYTE));
        }
        return result;
    }

    @Override
    public boolean isNullValue() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.isNullValue();
        }
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

    /**
     * Determines whether the tagless integer starting at `valueMarker.startIndex` and ending at `valueMarker.endIndex`
     * crosses a type boundary. Callers must only invoke this method when the integer's size is known to be either
     * 4 or 8 bytes.
     * @return true if the value fits in the Java integer type that matches its Ion serialized size; false if it
     *  requires the next larger size.
     */
    private boolean classifyFixedWidthTaglessInteger_1_1() {
        if (!taglessType.isUnsigned || taglessType.typeID.variableLength) {
            return true;
        }
        // UInt values with the most significant bit set will not fit in the signed Java primitive of the same width.
        return buffer[(int) valueMarker.endIndex - 1] >= 0;
    }

    /**
     * Selects and returns the size of the current integer value from the given options. Callers must only invoke this
     * method when the integer's size is known to be either 4 or 8 bytes, and it is the caller's responsibility to
     * provide correct values to 'smaller' and 'larger'.
     * @param smaller the smaller of the possible sizes.
     * @param larger the larger of the possible sizes.
     * @return the matching size.
     */
    private IntegerSize classifyFixedWidthInteger(IntegerSize smaller, IntegerSize larger) {
        if (minorVersion == 0) {
            return classifyInteger_1_0() ? smaller : larger;
        }
        if (taglessType == null) {
            return smaller;
        }
        return classifyFixedWidthTaglessInteger_1_1() ? smaller : larger;
    }

    // The maximum most-significant byte of a positive 5-byte FlexUInt or FlexUInt value that can fit in
    // a Java int. Integer.MAX_VALUE is 0x7FFFFFFF and a 5-byte Flex integer requires a right-shift of 5 bits.
    // 0x0FFF... >> 5 == 0x007F..., so all less significant byte values are guaranteed to fit and therefore do not
    // need to be examined individually.
    private static final int MAX_POSITIVE_FLEX_MSB_JAVA_INT = 0x0F;

    // The maximum most-significant byte of a positive 10-byte FlexUInt or FlexUInt value that can fit in
    // a Java long. Long.MAX_VALUE is 0x7FFFFFFFFFFFFFFF and a 10-byte Flex integer requires a right-shift of 10 bits.
    // 0x01FFFF... >> 10 == 0x00007F..., so all less significant byte values are guaranteed to fit and therefore do not
    // need to be examined individually.
    private static final int MAX_POSITIVE_FLEX_MSB_JAVA_LONG = 0x01;

    // The minimum most-significant byte of a negative 5-byte FlexInt with that can fit in a Java int.
    // Integer.MIN_VALUE is 0x80000000 and a 5-byte FlexInt requires a right-shift of 5 bits.
    // (int)(0xF000... >> 5) == 0x80... Any bits set in the less significant bytes would lessen the magnitude
    // and therefore do not need to be examined individually.
    private static final int MIN_NEGATIVE_FLEX_MSB_JAVA_INT = (byte) 0xF0;

    // The minimum most-significant byte of a negative 10-byte FlexInt with that can fit in a Java long.
    // Long.MIN_VALUE is 0x8000000000000000 and a 10-byte FlexInt requires a right-shift of 10 bits.
    // (long) (0xFE0000... >> 10) == 0x80... Any bits set in the less significant bytes would lessen the magnitude
    // and therefore do not need to be examined individually.
    private static final int MIN_NEGATIVE_FLEX_MSB_JAVA_LONG = (byte) 0xFE;

    /**
     * Classifies a 5- or 10-byte FlexInt or FlexUInt according the Java integer size required to represent it without
     * data loss.
     * @param maxPositiveMsb the maximum most-significant byte of a positive encoded integer that would allow the
     *                       value to fit in the smaller of the two Java types applicable to the relevant boundary.
     * @param minNegativeMsb the minimum most-significant byte of a negative encoded integer that would allow the
     *                       value to fit in the smaller of the two Java types applicable to the relevant boundary.
     * @return true if the encoded value fits in the smaller of the two Java types applicable to the relevant boundary;
     *  otherwise, false.
     */
    private boolean classifyVariableWidthTaglessIntegerAtBoundary_1_1(int maxPositiveMsb, int minNegativeMsb) {
        int mostSignificantByte = buffer[(int) valueMarker.endIndex - 1];
        if (taglessType.isUnsigned) {
            return (mostSignificantByte & SINGLE_BYTE_MASK) <= maxPositiveMsb;
        }
        return mostSignificantByte >= minNegativeMsb && mostSignificantByte <= maxPositiveMsb;
    }

    /**
     * Classifies the current variable-length integer (FlexInt or FlexUInt) according to the IntegerSize required to
     * represent it without data loss. For efficiency, does not attempt to find the smallest-possible size for
     * overpadded representations.
     * @param length the byte length of the FlexInt or FlexUInt to classify.
     * @return an IntegerSize capable of holding the value without data loss.
     */
    private IntegerSize classifyVariableWidthTaglessInteger_1_1(int length) {
        if (length < 5) {
            // Flex integers of less than 5 bytes cannot hit the Java int boundaries.
            return IntegerSize.INT;
        }
        if (length == 5) {
            return classifyVariableWidthTaglessIntegerAtBoundary_1_1(MAX_POSITIVE_FLEX_MSB_JAVA_INT, MIN_NEGATIVE_FLEX_MSB_JAVA_INT)
                ? IntegerSize.INT
                : IntegerSize.LONG;
        }
        if (length < 10) {
            // Flex integers of less than 10 bytes cannot hit the Java long boundaries.
            return IntegerSize.LONG;
        }
        if (length == 10) {
            return classifyVariableWidthTaglessIntegerAtBoundary_1_1(MAX_POSITIVE_FLEX_MSB_JAVA_LONG, MIN_NEGATIVE_FLEX_MSB_JAVA_LONG)
                ? IntegerSize.LONG
                : IntegerSize.BIG_INTEGER;
        }
        return IntegerSize.BIG_INTEGER;
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getIntegerSize();
        }
        if (valueTid == null || valueTid.type != IonType.INT || valueTid.isNull) {
            return null;
        }
        prepareScalar();
        int length;
        if (valueTid.variableLength) {
            length = (int) (valueMarker.endIndex - valueMarker.startIndex);
            if (taglessType != null) {
                // FlexUInt or FlexInt
                return classifyVariableWidthTaglessInteger_1_1(length);
            }
        } else {
            length = valueTid.length;
        }
        if (length < 0) {
            return IntegerSize.BIG_INTEGER;
        } else if (length < INT_SIZE_IN_BYTES) {
            return IntegerSize.INT;
        } else if (length == INT_SIZE_IN_BYTES) {
            return classifyFixedWidthInteger(IntegerSize.INT, IntegerSize.LONG);
        } else if (length < LONG_SIZE_IN_BYTES) {
            return IntegerSize.LONG;
        } else if (length == LONG_SIZE_IN_BYTES) {
            return classifyFixedWidthInteger(IntegerSize.LONG, IntegerSize.BIG_INTEGER);
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.byteSize();
        }
        if (valueTid == null || !IonType.isLob(valueTid.type) || valueTid.isNull) {
            throw new IonException("Reader must be positioned on a blob or clob.");
        }
        prepareScalar();
        return (int) (valueMarker.endIndex - valueMarker.startIndex);
    }

    @Override
    public byte[] newBytes() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.newBytes();
        }
        byte[] bytes = new byte[byteSize()];
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) valueMarker.startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public int getBytes(byte[] bytes, int offset, int len) {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getBytes(bytes, offset, len);
        }
        int length = Math.min(len, byteSize() - lobBytesRead);
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        System.arraycopy(buffer, (int) (valueMarker.startIndex + lobBytesRead), bytes, offset, length);
        lobBytesRead += length;
        return length;
    }

    @Override
    public void resetEncodingContext() {
        resetSymbolTable();
        int minorVersion = getIonMinorVersion();
        resetImports(getIonMajorVersion(), minorVersion);
        if (minorVersion > 0) {
            // TODO reset macro table
            installSymbols(SystemSymbols_1_1.allSymbolTexts());
        }
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.bigDecimalValue();
        }
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
        } else if (valueTid.type == IonType.FLOAT) {
            scalarConverter.addValue(doubleValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.decimal_value));
            value = scalarConverter.getDecimal();
            scalarConverter.clear();
        } else {
            throwDueToInvalidType(IonType.DECIMAL);
        }
        return value;
    }

    @Override
    public Decimal decimalValue() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.decimalValue();
        }
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
        } else if (valueTid.type == IonType.FLOAT) {
            scalarConverter.addValue(doubleValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.longValue();
        }
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.bigIntegerValue();
        }
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
                return sign == 0 ? 0e0 : -0e0;
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.doubleValue();
        }
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.timestampValue();
        }
        if (valueTid == null || IonType.TIMESTAMP != valueTid.type) {
            throwDueToInvalidType(IonType.TIMESTAMP);
        }
        if (valueTid.isNull) {
            return null;
        }
        prepareScalar();
        peekIndex = valueMarker.startIndex;
        if (peekIndex >= valueMarker.endIndex) {
            throw new IonException("Timestamp value cannot have length 0.");
        }
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
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.booleanValue();
        }
        if (valueTid == null || IonType.BOOL != valueTid.type || valueTid.isNull) {
            throwDueToInvalidType(IonType.BOOL);
        }
        prepareScalar();
        return minorVersion == 0 ? readBoolean_1_0() : readBoolean_1_1();
    }

    /**
     * Decodes the UTF-8 bytes between `valueMarker.startIndex` and `valueMarker.endIndex` into a String.
     * @return the value.
     */
    String readString() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.stringValue();
        }
        if (valueTid.isNull) {
            return null;
        }
        prepareScalar();
        ByteBuffer utf8InputBuffer = prepareByteBuffer(valueMarker.startIndex, valueMarker.endIndex);
        return utf8Decoder.decode(utf8InputBuffer, (int) (valueMarker.endIndex - valueMarker.startIndex));
    }

    @Override
    public String stringValue() {
        String value;
        IonType type = getEncodingType();
        if (type == IonType.STRING || isEvaluatingEExpression) {
            value = readString();
        } else if (type == IonType.SYMBOL) {
            if (valueTid.isInlineable) {
                value = readString();
            } else if (valueTid == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                value = getSymbolText();
            } else {
                int sid = symbolValueId();
                if (sid < 0) {
                    // The raw reader uses this to denote null.symbol.
                    return null;
                }
                value = getSymbol(sid);
                if (value == null) {
                    throw new UnknownSymbolException(sid);
                }
            }
        } else {
            throw new IllegalStateException("Invalid type requested.");
        }
        return value;
    }

    @Override
    public boolean hasSymbolText() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getType() == IonType.SYMBOL && !macroEvaluatorIonReader.isNullValue();
        }
        if (valueTid == null || IonType.SYMBOL != valueTid.type) {
            return false;
        }
        return valueTid.isInlineable || valueTid == IonTypeID.SYSTEM_SYMBOL_VALUE;
    }

    @Override
    public String getSymbolText() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.symbolValue().assumeText();
        }
        if (valueMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
            return getSystemSymbolToken(valueMarker).getText();
        }
        return readString();
    }

    @Override
    public int symbolValueId() {
        if (isEvaluatingEExpression) {
            if (macroEvaluatorIonReader.getType() != IonType.SYMBOL || macroEvaluatorIonReader.isNullValue()) {
                throwDueToInvalidType(IonType.SYMBOL);
            }
            return macroEvaluatorIonReader.symbolValue().getSid();
        }
        if (valueTid == null || IonType.SYMBOL != valueTid.type) {
            throwDueToInvalidType(IonType.SYMBOL);
        }
        if (valueTid.isNull) {
            return -1;
        }
        prepareScalar();
        if (minorVersion == 0) {
            return (int) readUInt(valueMarker.startIndex, valueMarker.endIndex);
        } else {
            if (taglessType != null) {
                // It is the caller's responsibility to call 'symbolValueId()' only when 'hasSymbolText()' is false,
                // meaning that the tagless FlexSym is encoded as a FlexInt representing a symbol ID.
                peekIndex = valueMarker.startIndex;
                return (int) readFlexInt_1_1();
            }
            if (valueTid.length == 1){
                return (int) readFixedUInt_1_1(valueMarker.startIndex, valueMarker.endIndex);
            } else if (valueTid.length == 2){
                return (int) readFixedUInt_1_1(valueMarker.startIndex, valueMarker.endIndex) + 256;
            } else if (valueTid.length == -1) {
                peekIndex = valueMarker.startIndex;
                return (int) readFlexUInt_1_1() + 65792;
            } else {
                throw new IllegalStateException("Illegal length " + valueTid.length + " for " + valueMarker);
            }
        }
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

    /**
     * Creates a SymbolToken representation of the given symbol ID.
     * @param sid a symbol ID.
     * @return a SymbolToken.
     */
    protected SymbolToken getSymbolToken(int sid) {
        return new SymbolTokenImpl(getSymbol(sid), sid);
    }

    protected final SymbolToken getSystemSymbolToken(Marker marker) {
        long id;
        if (marker.startIndex == -1) {
            id = marker.endIndex;
        } else {
            prepareScalar();
            id = readFixedUInt_1_1(marker.startIndex, marker.endIndex);

            // FIXME: This is a hack that works as long as our system symbol table doesn't grow to
            //  more than ~95 symbols. We need this hack because when we have to read the FixedInt,
            //  we don't know whether it's a tagless FlexSym or a Regular value.
            //  Possible solutions include:
            //     * changing the spec so that FlexSym System SIDs line up with the regular System SIDs
            //     * Introducing a dummy IonTypeID that indicates that we need to add the bias
            //     * Update IonCursorBinary.slowSkipFlexSym_1_1() to put the id into valueMarker.endIndex,
            //       though that seems to have its own problems.
            if (id >= FLEX_SYM_SYSTEM_SYMBOL_OFFSET) {
                id = id - FLEX_SYM_SYSTEM_SYMBOL_OFFSET;
            }
        }
        // In some cases, we pretend that $0 is a system symbol, so we must handle it here.
        if (id == 0) {
            return _Private_Utils.SYMBOL_0;
        }
        SystemSymbols_1_1 systemSymbol = SystemSymbols_1_1.get((int) id);
        if (systemSymbol == null) {
            throw new IonException("Unknown system symbol ID: " + id);
        }
        return systemSymbol.getToken();
    }

    @Override
    public void consumeAnnotationTokens(Consumer<SymbolToken> consumer) {
        if (annotationSequenceMarker.startIndex >= 0) {
            if (annotationSequenceMarker.typeId != null && annotationSequenceMarker.typeId.isInlineable) {
                getAnnotationMarkerList();
            } else {
                getAnnotationSidList();
                for (int i = 0; i < annotationSids.size(); i++) {
                    consumer.accept(getSymbolToken(annotationSids.get(i)));
                }
            }
        }
        for (int i = 0; i < annotationTokenMarkers.size(); i++) {
            Marker marker = annotationTokenMarkers.get(i);
            if (marker.startIndex < 0) {
                // This means the endIndex represents the token's symbol ID.
                if (minorVersion == 1 && marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                    consumer.accept(getSystemSymbolToken(marker));
                } else {
                    consumer.accept(getSymbolToken((int) marker.endIndex));
                }
            } else {
                // The token is inline UTF-8 text.
                ByteBuffer utf8InputBuffer = prepareByteBuffer(marker.startIndex, marker.endIndex);
                consumer.accept(new SymbolTokenImpl(utf8Decoder.decode(utf8InputBuffer, (int) (marker.endIndex - marker.startIndex)), -1));
            }
        }
    }

    /**
     * Gets the annotation markers for the current value, reading them from the buffer first if necessary.
     * @return the annotation markers, or an empty list if the current value is not annotated.
     */
    MarkerList getAnnotationMarkerList() {
        annotationTokenMarkers.clear();
        long savedPeekIndex = peekIndex;
        peekIndex = annotationSequenceMarker.startIndex;
        while (peekIndex < annotationSequenceMarker.endIndex) {
            Marker provisionalMarker = annotationTokenMarkers.provisionalElement();
            int annotationSid = (int) readFlexSym_1_1(provisionalMarker);
            if (annotationSid >= 0) {
                provisionalMarker.endIndex = annotationSid;
            } else if (provisionalMarker.endIndex < 0) {
                break;
            }
            annotationTokenMarkers.commit();
        }
        peekIndex = savedPeekIndex;
        return annotationTokenMarkers;
    }

    @Override
    public int getFieldId() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getFieldId();
        }
        return fieldSid;
    }

    @Override
    public boolean hasFieldText() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getFieldName() != null;
        }
        return fieldTextMarker.startIndex > -1 || fieldTextMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE;
    }

    @Override
    public String getFieldText() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getFieldName();
        }
        if (fieldTextMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
            return getSystemSymbolToken(fieldTextMarker).getText();
        }
        ByteBuffer utf8InputBuffer = prepareByteBuffer(fieldTextMarker.startIndex, fieldTextMarker.endIndex);
        return utf8Decoder.decode(utf8InputBuffer, (int) (fieldTextMarker.endIndex - fieldTextMarker.startIndex));
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getFieldNameSymbol();
        }
        if (fieldTextMarker.startIndex > -1) {
            return new SymbolTokenImpl(getFieldText(), SymbolTable.UNKNOWN_SYMBOL_ID);
        }
        if (fieldTextMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
            return getSystemSymbolToken(fieldTextMarker);
        }
        if (fieldSid < 0) {
            return null;
        }
        return getSymbolToken(fieldSid);
    }

    @Override
    public SymbolToken symbolValue() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.symbolValue();
        }
        if (valueTid == SYSTEM_SYMBOL_VALUE) {
            return getSystemSymbolToken(valueMarker);
        }
        if (valueTid.isInlineable) {
            return new SymbolTokenImpl(getSymbolText(), SymbolTable.UNKNOWN_SYMBOL_ID);
        }

        int sid = symbolValueId();
        if (sid < 0) {
            // The raw reader uses this to denote null.symbol.
            return null;
        }
        return getSymbolToken(sid);
    }

    @Override
    public boolean isInStruct() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.isInStruct();
        }
        return parent != null && parent.typeId.type == IonType.STRUCT;
    }

    @Override
    public final IonType getEncodingType() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getType();
        }
        return valueTid == null ? null : valueTid.type;
    }

    @Override
    public IonType getType() {
        if (isEvaluatingEExpression) {
            return macroEvaluatorIonReader.getType();
        }
        return valueTid == null ? null : valueTid.type;
    }

    @Override
    public int getDepth() {
        if (isEvaluatingEExpression) {
            return containerIndex + 1 + macroEvaluatorIonReader.getDepth();
        }
        return containerIndex + 1;
    }

    @Override
    public void close() {
        if (macroEvaluatorIonReader != null) {
            macroEvaluatorIonReader.close();
        }
        utf8Decoder.close();
        super.close();
    }
}
