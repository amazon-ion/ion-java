/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.Timestamp;
import software.amazon.ion.Timestamp.Precision;

/**
 * Class that generates {@link IonValue} based off configured properties.
 * <p>
 * Configuration points include:
 * <ul>
 *      <li><b>delta</b>:
 *      delta value expected between each generated IonValue</li>
 *      <li><b>size</b>:
 *      expected number of IonValues to be generated</li>
 *      <li><b>baseValue</b>:
 *      base IonValue from which the other generated IonValues are to be
 *      delta-ed from</li>
 *      <li><b>deltaType</b>:
 *      the type of the delta between each generated IonValue</li>
 * </ul>
 * <p>
 * This class can only be instantiated by the Builder pattern.
 *
 * @see Builder
 */
public class IonValueDeltaGenerator
{
    private final IonSystem ionSystem;
    private final int delta;
    private final int size;
    private final IonValue baseValue;
    private final Object deltaType;

    private final IonType valueType;

    public enum IonIntDeltaType {
        LONG,
        BIGINTEGER,
    }

    public enum IonFloatDeltaType {
        DOUBLE_ONE,
        DOUBLE_10_DECIMAL_DIGIT_PRECISION,
    }

    public enum IonDecimalDeltaType {
        BIGDECIMAL_SCALE_0,
        BIGDECIMAL_SCALE_1,
        BIGDECIMAL_SCALE_2,
        BIGDECIMAL_SCALE_4,
        BIGDECIMAL_SCALE_8,
        BIGDECIMAL_SCALE_16,
    }

    public enum IonSymbolDeltaType {
        TEXT,
        SID,
    }

    public enum IonTimestampDeltaType {
        SECOND_YEAR,
        SECOND_MONTH,
        SECOND_DAY,
        SECOND_MINUTE,
        SECOND_SECOND,
        YEAR,
        MONTH,
        DAY,
        MINUTE,
        SECOND,
    }

    /**
     * Builder pattern for easier readability and writability. Configurable
     * with required and/or optional parameters.
     */
    public static class Builder {
        private IonSystem ionSystem;
        private int delta;
        private int size;
        private IonValue baseValue;
        private Object deltaType;

        public Builder() {

        }

        /**
         * Configures the IonSystem that is the entry point for the DOM.
         */
        public Builder ionSystem(IonSystem val) {
            this.ionSystem = val;
            return this;
        }

        /**
         * Configures the delta value expected between each generated IonValue.
         *
         * @throws IllegalArgumentException if {@code val} is 0
         */
        public Builder delta(int val) {
            if (val == 0) {
                throw new IllegalArgumentException("0 is not a valid delta");
            }
            this.delta = val;
            return this;
        }

        /**
         * Configures the expected number of IonValues to be generated.
         *
         * @throws IllegalArgumentException if {@code val} is 0
         */
        public Builder size(int val) {
            if (val == 0) {
                throw new IllegalArgumentException("0 is not a valid size");
            }
            this.size = val;
            return this;
        }

        /**
         * Configures the base IonValue from which the other generated IonValues
         * are to be delta-ed from, inherting the same IonType.
         */
        public Builder baseValue(IonValue val) {
            this.baseValue = val;
            return this;
        }

        /**
         * Configures the type of the delta between each generated IonValue.
         *
         * @see #delta(int)
         */
        public Builder deltaType(Object val) {
            this.deltaType = val;
            return this;
        }

        /**
         * Builds a new {@link IonValueDeltaGenerator} instance based on
         * this builder's configuration properties.
         */
        public IonValueDeltaGenerator build() {
            return new IonValueDeltaGenerator(this);
        }
    }

    // Private constructor - only the Builder class can instantiate this class.
    private IonValueDeltaGenerator(Builder builder) {
        this.ionSystem  = builder.ionSystem;
        this.delta      = builder.delta;
        this.size       = builder.size;
        this.deltaType  = builder.deltaType;
        this.baseValue  = builder.baseValue;

        validateBaseValue(baseValue);
        this.valueType = baseValue.getType();
    }

    private void validateBaseValue(IonValue val) throws IllegalArgumentException
    {
        switch (val.getType())
        {
            // Only these IonTypes are supported
            case INT:
                validateIonInt((IonInt) baseValue);
                break;
            case DECIMAL:
            case FLOAT:
            case SYMBOL:
            case TIMESTAMP:
                break;
            default:
                throw new
                IllegalArgumentException("IonType not supported: " + val);
        }
    }

    // This is sync'ed with IonIntLite and IonIntImpl
    private static final BigInteger LONG_MIN_VALUE =
        BigInteger.valueOf(Long.MIN_VALUE);

    // This is sync'ed with IonIntLite and IonIntImpl
    private static final BigInteger LONG_MAX_VALUE =
        BigInteger.valueOf(Long.MAX_VALUE);

    private void validateIonInt(IonInt val) throws IllegalArgumentException
    {
        BigInteger baseInt = ((IonInt) baseValue).bigIntegerValue();

        // This is sync'ed with IonIntLite.doSetValue(BigInteger)
        // and IonIntImpl.doSetValue(BigInteger)
        if ((baseInt.compareTo(LONG_MIN_VALUE) > 0) &&
            (baseInt.compareTo(LONG_MAX_VALUE) < 0) &&
            ((IonIntDeltaType) deltaType).equals(IonIntDeltaType.BIGINTEGER))
        {
            fail("IonInt is tested on the BigInteger dimension but the " +
                        "value isn't sufficiently large to be backed by a " +
                        "BigInteger");
        }
    }

    /**
     * @return the IonType of the base IonValue
     */
    public IonType getValueType()
    {
        return valueType;
    }

    /**
     * Generate a Set of IonValues from the configuration properties that
     * were set.
     *
     * @return the set of IonValues
     */
    public Set<IonValue> generateValues()
    {
        switch (valueType)
        {
            case DECIMAL:
                return generateIonDecimals();
            case FLOAT:
                return generateIonFloats();
            case INT:
                return generateIonInts();
            case SYMBOL:
                return generateIonSymbols();
            case TIMESTAMP:
                return generateIonTimestamps();
            default:
                fail("not supported: " + valueType);
                return null;
        }
    }

    private Set<IonValue> generateIonDecimals()
    {
        Set<IonValue> dataSet = new HashSet<IonValue>();

        int scale = 0;
        switch ((IonDecimalDeltaType) deltaType)
        {
            case BIGDECIMAL_SCALE_0:
                scale = 0;
                break;
            case BIGDECIMAL_SCALE_1:
                scale = 1;
                break;
            case BIGDECIMAL_SCALE_2:
                scale = 2;
                break;
            case BIGDECIMAL_SCALE_4:
                scale = 4;
                break;
            case BIGDECIMAL_SCALE_8:
                scale = 8;
                break;
            case BIGDECIMAL_SCALE_16:
                scale = 16;
                break;
            default:
                fail("not supported: " + deltaType);
        }

        BigDecimal decimalDelta = BigDecimal.valueOf(delta, scale);

        IonDecimal curr = (IonDecimal) baseValue;
        for (int i = 0; i < size; i++)
        {
            BigDecimal nextBigDecimal = curr.decimalValue().add(decimalDelta);

            IonDecimal next = ionSystem.newDecimal(nextBigDecimal);

            boolean added = dataSet.add(next);
            assertTrue(added);
            curr = next;
        }

        return dataSet;
    }

    private Set<IonValue> generateIonFloats()
    {
        Set<IonValue> dataSet = new HashSet<IonValue>();

        double floatDelta = 0;
        switch ((IonFloatDeltaType) deltaType)
        {
            case DOUBLE_10_DECIMAL_DIGIT_PRECISION:
                floatDelta = delta * 1.0e-10d;
                break;
            case DOUBLE_ONE:
                floatDelta = delta * 1.0d;
                break;
            default:
                fail("not supported: " + deltaType);
        }

        double curr = ((IonFloat) baseValue).doubleValue();
        for (int i = 0; i < size; i++)
        {
            IonFloat next = ionSystem.newFloat(curr + floatDelta);

            boolean added = dataSet.add(next);
            assertTrue(added);
            curr = next.doubleValue();
        }

        return dataSet;
    }

    private Set<IonValue> generateIonInts()
    {
        Set<IonValue> dataSet = new HashSet<IonValue>();

        BigInteger intDelta = null;
        switch ((IonIntDeltaType) deltaType)
        {
            case BIGINTEGER:
            case LONG:
                intDelta = BigInteger.valueOf(delta);
                break;
            default:
                fail("not supported: " + deltaType);
        }

        IonInt curr = (IonInt) baseValue;
        for (int i = 0; i < size; i++)
        {
            BigInteger nextBigInteger = curr.bigIntegerValue().add(intDelta);

            IonInt next = ionSystem.newInt(nextBigInteger);

            boolean added = dataSet.add(next);
            assertTrue(added);
            curr = next;
        }

        return dataSet;
    }

    private Set<IonValue> generateIonSymbols()
    {
        Set<IonValue> dataSet = new HashSet<IonValue>();

        IonSymbol curr = (IonSymbol) baseValue;
        for (int i = 0; i < size; i++)
        {
            FakeSymbolToken nextToken = null;
            switch ((IonSymbolDeltaType) deltaType)
            {
                case SID:
                    nextToken =
                        new FakeSymbolToken(null, curr.symbolValue().getSid() + delta);
                    break;
                case TEXT: // TODO How should we test for text deltas?
                default:
                    fail("not supported: " + deltaType);
            }

            IonSymbol next = ionSystem.newSymbol(nextToken);

            boolean added = dataSet.add(next);
            assertTrue(added);
            curr = next;
        }

        return dataSet;
    }

    private Set<IonValue> generateIonTimestamps()
    {
        List<IonValue> dataList = new ArrayList<IonValue>();

        Precision precisionToSet = null;
        Precision precisionToDelta = null;

        switch ((IonTimestampDeltaType) deltaType)
        {
            case YEAR:
                precisionToSet = precisionToDelta = Precision.YEAR;
                break;
            case MONTH:
                precisionToSet = precisionToDelta = Precision.MONTH;
                break;
            case DAY:
                precisionToSet = precisionToDelta = Precision.DAY;
                break;
            case MINUTE:
                precisionToSet = precisionToDelta = Precision.MINUTE;
                break;
            case SECOND:
                precisionToSet = precisionToDelta = Precision.SECOND;
                break;
            case SECOND_YEAR:
                precisionToSet = Precision.SECOND;
                precisionToDelta = Precision.YEAR;
                break;
            case SECOND_MONTH:
                precisionToSet = Precision.SECOND;
                precisionToDelta = Precision.MONTH;
                break;
            case SECOND_DAY:
                precisionToSet = Precision.SECOND;
                precisionToDelta = Precision.DAY;
                break;
            case SECOND_MINUTE:
                precisionToSet = Precision.SECOND;
                precisionToDelta = Precision.MINUTE;
                break;
            case SECOND_SECOND:
                precisionToSet = Precision.SECOND;
                precisionToDelta = Precision.SECOND;
                break;
            default:
                fail("not supported: " + deltaType);
        }

        IonTimestamp curr = (IonTimestamp) baseValue;
        for (int i = 0; i < size; i++)
        {
            IonTimestamp next = getIonTimestampWithDelta(curr.timestampValue(),
                                                         precisionToDelta,
                                                         precisionToSet,
                                                         delta);

            // There might be equal IonTimestamps generated due to there being
            // no reliable way to generate IonTimestamp deltas. As such, we
            // append to a List first, and convert it to a Set as the result to
            // return.
            dataList.add(next);
            curr = next;
        }

        return new HashSet<IonValue>(dataList);
    }

    /**
     * Use {@link Calendar#add(int, int)} method to create IonTimestamp deltas.
     * This is a workaround as there is no available way to create IonTimestamp
     * with increments from another IonTimestamp.
     * <p>
     * NOTE: {@link Calendar#add(int, int)} doesn't reliable generate different
     * Calendar instances as it normalizes in its internal implementation with
     * regards to unset fields. See {@link #generateIonTimestamps()} for the
     * workaround on creating unique IonTimestamp deltas.
     */
    private IonTimestamp getIonTimestampWithDelta(Timestamp currTimestamp,
                                                  Precision precisionToDelta,
                                                  Precision precisionToSet,
                                                  int delta)
    {
        Calendar cal = currTimestamp.calendarValue();

        switch (precisionToDelta)
        {
            case YEAR:
                cal.add(Calendar.YEAR, delta);
                break;
            case MONTH:
                cal.add(Calendar.MONTH, delta);
                break;
            case DAY:
                cal.add(Calendar.DAY_OF_MONTH, delta);
                break;
            case MINUTE:
                cal.add(Calendar.MINUTE, delta);
                break;
            case SECOND:
                cal.add(Calendar.MILLISECOND, delta);
                break;
            default:
                fail("not supported: " + precisionToDelta);
        }

        switch (precisionToSet)
        {
            case YEAR:
                cal.clear(Calendar.MONTH);
            case MONTH:
                cal.clear(Calendar.DAY_OF_MONTH);
            case DAY:
                cal.clear(Calendar.MINUTE);
                cal.clear(Calendar.HOUR_OF_DAY);
            case MINUTE:
                cal.clear(Calendar.SECOND);
            case SECOND:
                cal.clear(Calendar.MILLISECOND);
                break;
            default:
                fail("not supported: " + precisionToSet);
        }

        Timestamp nextTimestamp = Timestamp.forCalendar(cal);
        assertTrue(nextTimestamp.getPrecision().equals(precisionToSet));

        return ionSystem.newTimestamp(nextTimestamp);
    }

}
