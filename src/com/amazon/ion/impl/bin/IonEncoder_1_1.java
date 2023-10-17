package com.amazon.ion.impl.bin;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.utf8.Utf8StringEncoder;

import java.math.BigDecimal;
import java.math.BigInteger;

import static java.lang.Double.doubleToRawLongBits;
import static java.lang.Float.floatToIntBits;

/**
 * Provides functions for writing various Ion values to a WriteBuffer.
 *
 * This class can be subsumed by IonRawBinaryWriter_1_1, when it is created.
 */
public class IonEncoder_1_1 {

    /**
     * Writes an Ion Null value to the given WriteBuffer.
     * @return the number of bytes written
     */
    public static int writeNullValue(WriteBuffer buffer, final IonType ionType) {
        if (ionType == IonType.NULL) {
            buffer.writeByte(OpCodes.NULL_UNTYPED);
            return 1;
        }

        buffer.writeByte(OpCodes.NULL_TYPED);
        switch (ionType) {
            case BOOL:
                buffer.writeByte((byte) 0x00);
                break;
            case INT:
                buffer.writeByte((byte) 0x01);
                break;
            case FLOAT:
                buffer.writeByte((byte) 0x02);
                break;
            case DECIMAL:
                buffer.writeByte((byte) 0x03);
                break;
            case TIMESTAMP:
                buffer.writeByte((byte) 0x04);
                break;
            case STRING:
                buffer.writeByte((byte) 0x05);
                break;
            case SYMBOL:
                buffer.writeByte((byte) 0x06);
                break;
            case BLOB:
                buffer.writeByte((byte) 0x07);
                break;
            case CLOB:
                buffer.writeByte((byte) 0x08);
                break;
            case LIST:
                buffer.writeByte((byte) 0x09);
                break;
            case SEXP:
                buffer.writeByte((byte) 0x0A);
                break;
            case STRUCT:
                buffer.writeByte((byte) 0x0B);
                break;
            case DATAGRAM:
                throw new IllegalArgumentException("Cannot write a null datagram");
        }
        return 2;
    }

    /**
     * Writes an Ion Bool value to the given WriteBuffer.
     * @return the number of bytes written
     */
    public static int writeBoolValue(WriteBuffer buffer, final boolean value) {
        if (value) {
            buffer.writeByte(OpCodes.BOOLEAN_TRUE);
        } else {
            buffer.writeByte(OpCodes.BOOLEAN_FALSE);
        }
        return 1;
    }

    /**
     * Writes an Ion Integer value to the given WriteBuffer.
     * @return the number of bytes written
     */
    public static int writeIntValue(WriteBuffer buffer, final long value) {
        if (value == 0) {
            buffer.writeByte(OpCodes.INTEGER_ZERO_LENGTH);
            return 1;
        }
        int length = WriteBuffer.fixedIntLength(value);
        buffer.writeByte((byte) (OpCodes.INTEGER_ZERO_LENGTH + length));
        buffer.writeFixedInt(value);
        return 1 + length;
    }

    private static final BigInteger BIG_INT_LONG_MAX_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger BIG_INT_LONG_MIN_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    /**
     * Writes an Ion Integer value to the given WriteBuffer.
     * @return the number of bytes written
     */
    public static int writeIntValue(WriteBuffer buffer, final BigInteger value) {
        if (value == null) {
            return writeNullValue(buffer, IonType.INT);
        }
        if (value.compareTo(BIG_INT_LONG_MIN_VALUE) >= 0 && value.compareTo(BIG_INT_LONG_MAX_VALUE) <= 0) {
            return writeIntValue(buffer, value.longValue());
        }
        buffer.writeByte(OpCodes.VARIABLE_LENGTH_INTEGER);
        byte[] intBytes = value.toByteArray();
        int totalBytes = 1 + intBytes.length + buffer.writeFlexUInt(intBytes.length);
        for (int i = intBytes.length; i > 0; i--) {
            buffer.writeByte(intBytes[i-1]);
        }
        return totalBytes;
    }

    /**
     * Writes a float to the given WriteBuffer using the Ion 1.1 encoding for Ion Floats.
     * @return the number of bytes written
     */
    public static int writeFloat(WriteBuffer buffer, final float value) {
        // TODO: Optimization to write a 16 bit float for non-finite and possibly other values
        if (value == 0.0) {
            buffer.writeByte(OpCodes.FLOAT_ZERO_LENGTH);
            return 1;
        } else {
            buffer.writeByte(OpCodes.FLOAT_32);
            buffer.writeUInt32(floatToIntBits(value));
            return 5;
        }
    }

    /**
     * Writes a double to the given WriteBuffer using the Ion 1.1 encoding for Ion Floats.
     * @return the number of bytes written
     */
    public static int writeFloat(WriteBuffer buffer, final double value) {
        // TODO: Optimization to write a 16 bit float for non-finite and possibly other values
        if (value == 0.0) {
            buffer.writeByte(OpCodes.FLOAT_ZERO_LENGTH);
            return 1;
        } else if (!Double.isFinite(value) || value == (float) value) {
            buffer.writeByte(OpCodes.FLOAT_32);
            buffer.writeUInt32(floatToIntBits((float) value));
            return 5;
        } else {
            buffer.writeByte(OpCodes.FLOAT_64);
            buffer.writeUInt64(doubleToRawLongBits(value));
            return 9;
        }
    }
}
