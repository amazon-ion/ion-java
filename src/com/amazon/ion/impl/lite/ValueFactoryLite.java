// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.Decimal;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.Timestamp;
import com.amazon.ion.ValueFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *  This class handles all of the IonValueLite
 *  instance construction.
 *
 */
public abstract class ValueFactoryLite
    implements ValueFactory
{
    private IonSystemLite _system;

    protected void set_system(IonSystemLite system) {
        _system = system;
    }

    public IonBlobLite newBlob(byte[] value)
    {
        IonBlobLite ionValue = newBlob(value, 0, value == null ? 0 : value.length);
        return ionValue;
    }

    public IonBlobLite newBlob(byte[] value, int offset, int length)
    {
        IonBlobLite ionValue = new IonBlobLite(_system, (value == null));
        ionValue.setBytes(value, offset, length);
        return ionValue;
    }

    public IonBoolLite newBool(boolean value)
    {
        IonBoolLite ionValue = new IonBoolLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonBoolLite newBool(Boolean value)
    {
        IonBoolLite ionValue = new IonBoolLite(_system, (value == null));
        ionValue.setValue(value);
        return ionValue;
    }

    public IonClobLite newClob(byte[] value)
    {
        IonClobLite ionValue = newClob(value, 0, value == null ? 0 : value.length);
        return ionValue;
    }

    public IonClobLite newClob(byte[] value, int offset, int length)
    {
        IonClobLite ionValue = new IonClobLite(_system, (value == null));
        ionValue.setBytes(value, offset, length);
        return ionValue;
    }

    public IonDecimalLite newDecimal(long value)
    {
        IonDecimalLite ionValue = new IonDecimalLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonDecimalLite newDecimal(double value)
    {
        IonDecimalLite ionValue = new IonDecimalLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonDecimalLite newDecimal(BigInteger value)
    {
        boolean isNull = (value == null);
        IonDecimalLite ionValue = new IonDecimalLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(Decimal.valueOf(value));
        }
        return ionValue;
    }

    public IonDecimalLite newDecimal(BigDecimal value)
    {
        boolean isNull = (value == null);
        IonDecimalLite ionValue = new IonDecimalLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(value);
        }
        return ionValue;
    }

    public IonListLite newEmptyList()
    {
        IonListLite ionValue = new IonListLite(_system, false);
        return ionValue;
    }

    public IonSexpLite newEmptySexp()
    {
        IonSexpLite ionValue = new IonSexpLite(_system, false);
        return ionValue;
    }

    public IonStructLite newEmptyStruct()
    {
        IonStructLite ionValue = new IonStructLite(_system, false);
        return ionValue;
    }

    public IonFloatLite newFloat(long value)
    {
        IonFloatLite ionValue = new IonFloatLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonFloatLite newFloat(double value)
    {
        IonFloatLite ionValue = new IonFloatLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonIntLite newInt(int value)
    {
        IonIntLite ionValue = new IonIntLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonIntLite newInt(long value)
    {
        IonIntLite ionValue = new IonIntLite(_system, false);
        ionValue.setValue(value);
        return ionValue;
    }

    public IonIntLite newInt(Number value)
    {
        boolean isNull = (value == null);
        IonIntLite ionValue = new IonIntLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(value);
        }
        return ionValue;
    }

    public IonListLite newList(Collection<? extends IonValue> values)
        throws ContainedValueException, NullPointerException
    {
        IonListLite ionValue = newEmptyList();
        if (values == null) {
            ionValue.makeNull();
        }
        else {
            ionValue.addAll(values);
        }
        return ionValue;
    }

    public <T extends IonValueLite> IonListLite newList(T... values)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (values == null ? null : Arrays.asList(values));
        IonListLite ionValue = newEmptyList();
        if (e == null) {
            ionValue.makeNull();
        }
        else {
            ionValue.addAll(e);
        }
        return ionValue;
    }

    public IonListLite newList(int[] values)
    {
        ArrayList<IonIntLite> e = newInts(values);
        return newList(e);
    }

    public IonListLite newList(long[] values)
    {
        ArrayList<IonIntLite> e = newInts(values);
        return newList(e);
    }

    public IonNullLite newNull()
    {
        IonNullLite ionValue = new IonNullLite(_system);
        return ionValue;
    }

    public IonValueLite newNull(IonType type)
    {
        switch (type)
        {
            case NULL:          return newNull();
            case BOOL:          return newNullBool();
            case INT:           return newNullInt();
            case FLOAT:         return newNullFloat();
            case DECIMAL:       return newNullDecimal();
            case TIMESTAMP:     return newNullTimestamp();
            case SYMBOL:        return newNullSymbol();
            case STRING:        return newNullString();
            case CLOB:          return newNullClob();
            case BLOB:          return newNullBlob();
            case LIST:          return newNullList();
            case SEXP:          return newNullSexp();
            case STRUCT:        return newNullStruct();
            default:
                throw new IllegalArgumentException();
        }
    }

    public IonBlobLite newNullBlob()
    {
        IonBlobLite ionValue = new IonBlobLite(_system, true);
        return ionValue;
    }

    public IonBoolLite newNullBool()
    {
        IonBoolLite ionValue = new IonBoolLite(_system, true);
        return ionValue;
    }

    public IonClobLite newNullClob()
    {
        IonClobLite ionValue = new IonClobLite(_system, true);
        return ionValue;
    }

    public IonDecimalLite newNullDecimal()
    {
        IonDecimalLite ionValue = new IonDecimalLite(_system, true);
        return ionValue;
    }

    public IonFloatLite newNullFloat()
    {
        IonFloatLite ionValue = new IonFloatLite(_system, true);
        return ionValue;
    }

    public IonIntLite newNullInt()
    {
        IonIntLite ionValue = new IonIntLite(_system, true);
        return ionValue;
    }

    public IonListLite newNullList()
    {
        IonListLite ionValue = new IonListLite(_system, true);
        return ionValue;
    }

    public IonSexpLite newNullSexp()
    {
        IonSexpLite ionValue = new IonSexpLite(_system, true);
        return ionValue;
    }

    public IonStringLite newNullString()
    {
        IonStringLite ionValue = new IonStringLite(_system, true);
        return ionValue;
    }

    public IonStructLite newNullStruct()
    {
        IonStructLite ionValue = new IonStructLite(_system, true);
        return ionValue;
    }

    public IonSymbolLite newNullSymbol()
    {
        IonSymbolLite ionValue = new IonSymbolLite(_system, true);
        return ionValue;
    }

    public IonTimestampLite newNullTimestamp()
    {
        IonTimestampLite ionValue = new IonTimestampLite(_system, true);
        return ionValue;
    }

    public IonSexpLite newSexp(Collection<? extends IonValue> values)
        throws ContainedValueException, NullPointerException
    {
        IonSexpLite ionValue = newEmptySexp();
        if (values == null) {
            ionValue.makeNull();
        }
        else {
            ionValue.addAll(values);
        }
        return ionValue;
    }

    public <T extends IonValueLite> IonSexp newSexp(T... values)
        throws ContainedValueException, NullPointerException
    {
        List<T> e = (values == null ? null : Arrays.asList(values));
        IonSexpLite ionValue = newEmptySexp();
        if (e == null) {
            ionValue.makeNull();
        }
        else {
            ionValue.addAll(e);
        }
        return ionValue;
    }

    public IonSexpLite newSexp(int[] values)
    {
        ArrayList<IonIntLite> e = newInts(values);
        return newSexp(e);
    }

    public IonSexpLite newSexp(long[] values)
    {
        ArrayList<IonIntLite> e = newInts(values);
        return newSexp(e);
    }

    public IonStringLite newString(String value)
    {
        boolean isNull = (value == null);
        IonStringLite ionValue = new IonStringLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(value);
        }
        return ionValue;
    }

    public IonSymbolLite newSymbol(String value)
    {
        boolean isNull = (value == null);
        IonSymbolLite ionValue = new IonSymbolLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(value);
        }
        return ionValue;
    }

    public IonTimestampLite newTimestamp(Timestamp value)
    {
        boolean isNull = (value == null);
        IonTimestampLite ionValue = new IonTimestampLite(_system, isNull);
        if (value != null) {
            ionValue.setValue(value);
        }
        return ionValue;
    }

    private ArrayList<IonIntLite> newInts(int[] elements)
    {
        ArrayList<IonIntLite> e = null;

        if (elements != null)
        {
            e = new ArrayList<IonIntLite>(elements.length);
            for (int i = 0; i < elements.length; i++)
            {
                int value = elements[i];
                e.add(newInt(value));
            }
        }

        return e;
    }

    private ArrayList<IonIntLite> newInts(long[] elements)
    {
        ArrayList<IonIntLite> e = null;

        if (elements != null)
        {
            e = new ArrayList<IonIntLite>(elements.length);
            for (int i = 0; i < elements.length; i++)
            {
                long value = elements[i];
                e.add(newInt(value));
            }
        }

        return e;
    }
}
