package com.amazon.ion.impl.bin;

import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.amazon.ion.impl.bin.Ion_1_1_Constants.*;
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

    /**
     * Writes a Timestamp to the given WriteBuffer using the Ion 1.1 encoding for Ion Timestamps.
     * @return the number of bytes written
     */
    public static int writeTimestampValue(WriteBuffer buffer, Timestamp value) {
        if (value == null) {
            return writeNullValue(buffer, IonType.TIMESTAMP);
        }
        // Timestamps may be encoded using the short form if they meet certain conditions.
        // Condition 1: The year is between 1970 and 2097.
        if (value.getYear() < 1970 || value.getYear() > 2097) {
            return writeLongFormTimestampValue(buffer, value);
        }

        // If the precision is year, month, or day, we can skip the remaining checks.
        if (!value.getPrecision().includes(Timestamp.Precision.MINUTE)) {
            return writeShortFormTimestampValue(buffer, value);
        }

        // Condition 2: The fractional seconds are a common precision.
        if (value.getZFractionalSecond() != null) {
            int secondsScale = value.getZFractionalSecond().scale();
            if (secondsScale != 0 && secondsScale != 3 && secondsScale != 6 && secondsScale != 9) {
                return writeLongFormTimestampValue(buffer, value);
            }
        }
        // Condition 3: The local offset is either UTC, unknown, or falls between -14:00 to +14:00 and is divisible by 15 minutes.
        Integer offset = value.getLocalOffset();
        if (offset != null && (offset < -14 * 60 || offset > 14 * 60 || offset % 15 != 0)) {
            return writeLongFormTimestampValue(buffer, value);
        }
        return writeShortFormTimestampValue(buffer, value);
    }

    /**
     * Writes a short-form timestamp.
     * Value cannot be null.
     * If calling from outside this class, use writeTimestampValue instead.
     */
    private static int writeShortFormTimestampValue(WriteBuffer buffer, Timestamp value) {
        long bits = (value.getYear() - 1970L);
        if (value.getPrecision() == Timestamp.Precision.YEAR) {
            buffer.writeByte(OpCodes.TIMESTAMP_YEAR_PRECISION);
            buffer.writeFixedIntOrUInt(bits, 1);
            return 2;
        }

        bits |= ((long) value.getMonth()) << S_TIMESTAMP_MONTH_BIT_OFFSET;
        if (value.getPrecision() == Timestamp.Precision.MONTH) {
            buffer.writeByte(OpCodes.TIMESTAMP_MONTH_PRECISION);
            buffer.writeFixedIntOrUInt(bits, 2);
            return 3;
        }

        bits |= ((long) value.getDay()) << S_TIMESTAMP_DAY_BIT_OFFSET;
        if (value.getPrecision() == Timestamp.Precision.DAY) {
            buffer.writeByte(OpCodes.TIMESTAMP_DAY_PRECISION);
            buffer.writeFixedIntOrUInt(bits, 2);
            return 3;
        }

        bits |= ((long) value.getHour()) << S_TIMESTAMP_HOUR_BIT_OFFSET;
        bits |= ((long) value.getMinute()) << S_TIMESTAMP_MINUTE_BIT_OFFSET;
        if (value.getLocalOffset() == null || value.getLocalOffset() == 0) {
            if (value.getLocalOffset() != null) {
                bits |= S_U_TIMESTAMP_UTC_FLAG;
            }

            if (value.getPrecision() == Timestamp.Precision.MINUTE) {
                buffer.writeByte(OpCodes.TIMESTAMP_MINUTE_PRECISION);
                buffer.writeFixedIntOrUInt(bits, 4);
                return 5;
            }

            bits |= ((long) value.getSecond()) << S_U_TIMESTAMP_SECOND_BIT_OFFSET;

            int secondsScale = 0;
            if (value.getZFractionalSecond() != null) {
                secondsScale = value.getZFractionalSecond().scale();
            }
            if (secondsScale != 0) {
                long fractionalSeconds = value.getZFractionalSecond().unscaledValue().longValue();
                bits |= fractionalSeconds << S_U_TIMESTAMP_FRACTION_BIT_OFFSET;
            }
            switch (secondsScale) {
                case 0:
                    buffer.writeByte(OpCodes.TIMESTAMP_SECOND_PRECISION);
                    buffer.writeFixedIntOrUInt(bits, 5);
                    return 6;
                case 3:
                    buffer.writeByte(OpCodes.TIMESTAMP_MILLIS_PRECISION);
                    buffer.writeFixedIntOrUInt(bits, 6);
                    return 7;
                case 6:
                    buffer.writeByte(OpCodes.TIMESTAMP_MICROS_PRECISION);
                    buffer.writeFixedIntOrUInt(bits, 7);
                    return 8;
                case 9:
                    buffer.writeByte(OpCodes.TIMESTAMP_NANOS_PRECISION);
                    buffer.writeFixedIntOrUInt(bits, 8);
                    return 9;
                default:
                    throw new IllegalStateException("This is unreachable!");
            }
        } else {
            long localOffset = (value.getLocalOffset().longValue() / 15) + (14 * 4);
            bits |= (localOffset & LEAST_SIGNIFICANT_7_BITS) << S_O_TIMESTAMP_OFFSET_BIT_OFFSET;

            if (value.getPrecision() == Timestamp.Precision.MINUTE) {
                buffer.writeByte(OpCodes.TIMESTAMP_MINUTE_PRECISION_WITH_OFFSET);
                buffer.writeFixedIntOrUInt(bits, 5);
                return 6;
            }

            bits |= ((long) value.getSecond()) << S_O_TIMESTAMP_SECOND_BIT_OFFSET;

            // The fractional seconds bits will be put into a separate long because we need nine bytes total
            // if there are nanoseconds (which is too much for one long) and the boundary between the seconds
            // and fractional seconds subfields conveniently aligns with a byte boundary.
            long fractionBits = 0;
            int secondsScale = 0;
            if (value.getZFractionalSecond() != null) {
                secondsScale = value.getZFractionalSecond().scale();
            }
            if (secondsScale != 0) {
                fractionBits = value.getZFractionalSecond().unscaledValue().longValue();
            }
            switch (secondsScale) {
                case 0:
                    buffer.writeByte(OpCodes.TIMESTAMP_SECOND_PRECISION_WITH_OFFSET);
                    buffer.writeFixedIntOrUInt(bits, 5);
                    return 6;
                case 3:
                    buffer.writeByte(OpCodes.TIMESTAMP_MILLIS_PRECISION_WITH_OFFSET);
                    buffer.writeFixedIntOrUInt(bits, 5);
                    buffer.writeFixedIntOrUInt(fractionBits, 2);
                    return 8;
                case 6:
                    buffer.writeByte(OpCodes.TIMESTAMP_MICROS_PRECISION_WITH_OFFSET);
                    buffer.writeFixedIntOrUInt(bits, 5);
                    buffer.writeFixedIntOrUInt(fractionBits, 3);
                    return 9;
                case 9:
                    buffer.writeByte(OpCodes.TIMESTAMP_NANOS_PRECISION_WITH_OFFSET);
                    buffer.writeFixedIntOrUInt(bits, 5);
                    buffer.writeFixedIntOrUInt(fractionBits, 4);
                    return 10;
                default:
                    throw new IllegalStateException("This is unreachable!");
            }
        }
    }

    /**
     * Writes a long-form timestamp.
     * Value may not be null.
     * Only visible for testing. If calling from outside this class, use writeTimestampValue instead.
     */
    static int writeLongFormTimestampValue(WriteBuffer buffer, Timestamp value) {
        buffer.writeByte(OpCodes.VARIABLE_LENGTH_TIMESTAMP);

        long bits = value.getYear();
        if (value.getPrecision() == Timestamp.Precision.YEAR) {
            buffer.writeFlexUInt(2);
            buffer.writeFixedIntOrUInt(bits, 2);
            return 4; // OpCode + FlexUInt + 2 bytes data
        }

        bits |= ((long) value.getMonth()) << L_TIMESTAMP_MONTH_BIT_OFFSET;
        if (value.getPrecision() == Timestamp.Precision.MONTH) {
            buffer.writeFlexUInt(3);
            buffer.writeFixedIntOrUInt(bits, 3);
            return 5; // OpCode + FlexUInt + 3 bytes data
        }

        bits |= ((long) value.getDay()) << L_TIMESTAMP_DAY_BIT_OFFSET;
        if (value.getPrecision() == Timestamp.Precision.DAY) {
            buffer.writeFlexUInt(3);
            buffer.writeFixedIntOrUInt(bits, 3);
            return 5; // OpCode + FlexUInt + 3 bytes data
        }

        bits |= ((long) value.getHour()) << L_TIMESTAMP_HOUR_BIT_OFFSET;
        bits |= ((long) value.getMinute()) << L_TIMESTAMP_MINUTE_BIT_OFFSET;
        long localOffsetValue = L_TIMESTAMP_UNKNOWN_OFFSET_VALUE;
        if (value.getLocalOffset() != null) {
            localOffsetValue = value.getLocalOffset() + (24 * 60);
        }
        bits |= localOffsetValue << L_TIMESTAMP_OFFSET_BIT_OFFSET;

        if (value.getPrecision() == Timestamp.Precision.MINUTE) {
            buffer.writeFlexUInt(6);
            buffer.writeFixedIntOrUInt(bits, 6);
            return 8; // OpCode + FlexUInt + 6 bytes data
        }


        bits |= ((long) value.getSecond()) << L_TIMESTAMP_SECOND_BIT_OFFSET;
        int secondsScale = 0;
        if (value.getZFractionalSecond() != null) {
            secondsScale = value.getZFractionalSecond().scale();
        }
        if (secondsScale == 0) {
            buffer.writeFlexUInt(7);
            buffer.writeFixedIntOrUInt(bits, 7);
            return 9; // OpCode + FlexUInt + 7 bytes data
        }

        BigDecimal fractionalSeconds = value.getZFractionalSecond();
        BigInteger coefficient = fractionalSeconds.unscaledValue();
        long exponent = fractionalSeconds.scale();
        int numCoefficientBytes = WriteBuffer.flexUIntLength(coefficient);
        int numExponentBytes = WriteBuffer.fixedUIntLength(exponent);
        // Years-seconds data (7 bytes) + fraction coefficient + fraction exponent
        int dataLength = 7 + numCoefficientBytes + numExponentBytes;

        buffer.writeFlexUInt(dataLength);
        buffer.writeFixedIntOrUInt(bits, 7);
        buffer.writeFlexUInt(coefficient);
        buffer.writeFixedUInt(exponent);

        // OpCode + FlexUInt length + dataLength
        return 1 + WriteBuffer.flexUIntLength(dataLength) + dataLength;
    }

}
