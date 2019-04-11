/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.ValueFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Helper for implementing curried container insertion methods such as
 * {@link IonStruct#put(String)}.
 */
public abstract class _Private_CurriedValueFactory
    implements ValueFactory
{
    private final ValueFactory myFactory;

    /**
     * @param factory must not be null.
     */
    protected _Private_CurriedValueFactory(ValueFactory factory)
    {
        myFactory = factory;
    }

    /**
     * Subclasses override this to do something with each newly-constructed
     * value.
     *
     * @param newValue was just constructed by {@link #myFactory}.
     */
    protected abstract void handle(IonValue newValue);

    //-------------------------------------------------------------------------

    public IonBlob newNullBlob()
    {
        IonBlob v = myFactory.newNullBlob();
        handle(v);
        return v;
    }

    public IonBlob newBlob(byte[] value)
    {
        IonBlob v = myFactory.newBlob(value);
        handle(v);
        return v;
    }

    public IonBlob newBlob(byte[] value, int offset, int length)
    {
        IonBlob v = myFactory.newBlob(value, offset, length);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonBool newNullBool()
    {
        IonBool v = myFactory.newNullBool();
        handle(v);
        return v;
    }

    public IonBool newBool(boolean value)
    {
        IonBool v = myFactory.newBool(value);
        handle(v);
        return v;
    }

    public IonBool newBool(Boolean value)
    {
        IonBool v = myFactory.newBool(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonClob newNullClob()
    {
        IonClob v = myFactory.newNullClob();
        handle(v);
        return v;
    }

    public IonClob newClob(byte[] value)
    {
        IonClob v = myFactory.newClob(value);
        handle(v);
        return v;
    }

    public IonClob newClob(byte[] value, int offset, int length)
    {
        IonClob v = myFactory.newClob(value, offset, length);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonDecimal newNullDecimal()
    {
        IonDecimal v = myFactory.newNullDecimal();
        handle(v);
        return v;
    }

    public IonDecimal newDecimal(long value)
    {
        IonDecimal v = myFactory.newDecimal(value);
        handle(v);
        return v;
    }

    public IonDecimal newDecimal(double value)
    {
        IonDecimal v = myFactory.newDecimal(value);
        handle(v);
        return v;
    }

    public IonDecimal newDecimal(BigInteger value)
    {
        IonDecimal v = myFactory.newDecimal(value);
        handle(v);
        return v;
    }

    public IonDecimal newDecimal(BigDecimal value)
    {
        IonDecimal v = myFactory.newDecimal(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonFloat newNullFloat()
    {
        IonFloat v = myFactory.newNullFloat();
        handle(v);
        return v;
    }

    public IonFloat newFloat(long value)
    {
        IonFloat v = myFactory.newFloat(value);
        handle(v);
        return v;
    }

    public IonFloat newFloat(double value)
    {
        IonFloat v = myFactory.newFloat(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonInt newNullInt()
    {
        IonInt v = myFactory.newNullInt();
        handle(v);
        return v;
    }

    public IonInt newInt(int value)
    {
        IonInt v = myFactory.newInt(value);
        handle(v);
        return v;
    }

    public IonInt newInt(long value)
    {
        IonInt v = myFactory.newInt(value);
        handle(v);
        return v;
    }

    public IonInt newInt(Number value)
    {
        IonInt v = myFactory.newInt(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonList newNullList()
    {
        IonList v = myFactory.newNullList();
        handle(v);
        return v;
    }

    public IonList newEmptyList()
    {
        IonList v = myFactory.newEmptyList();
        handle(v);
        return v;
    }

    @Deprecated
    public IonList newList(Collection<? extends IonValue> values)
        throws ContainedValueException, NullPointerException
    {
        IonList v = myFactory.newList(values);
        handle(v);
        return v;
    }

    public IonList newList(IonSequence firstChild)
    throws ContainedValueException, NullPointerException
    {
        IonList v = myFactory.newList(firstChild);
        handle(v);
        return v;
    }

    public IonList newList(IonValue... values)
        throws ContainedValueException, NullPointerException
    {
        IonList v = myFactory.newList(values);
        handle(v);
        return v;
    }

    public IonList newList(int[] values)
    {
        IonList v = myFactory.newList(values);
        handle(v);
        return v;
    }

    public IonList newList(long[] values)
    {
        IonList v = myFactory.newList(values);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonNull newNull()
    {
        IonNull v = myFactory.newNull();
        handle(v);
        return v;
    }

    public IonValue newNull(IonType type)
    {
        IonValue v = myFactory.newNull(type);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonSexp newNullSexp()
    {
        IonSexp v = myFactory.newNullSexp();
        handle(v);
        return v;
    }

    public IonSexp newEmptySexp()
    {
        IonSexp v = myFactory.newEmptySexp();
        handle(v);
        return v;
    }

    @Deprecated
    public IonSexp newSexp(Collection<? extends IonValue> values)
        throws ContainedValueException, NullPointerException
    {
        IonSexp v = myFactory.newSexp(values);
        handle(v);
        return v;
    }

    public IonSexp newSexp(IonSequence firstChild)
    throws ContainedValueException, NullPointerException
    {
        IonSexp v = myFactory.newSexp(firstChild);
        handle(v);
        return v;
    }

    public IonSexp newSexp(IonValue... values)
        throws ContainedValueException, NullPointerException
    {
        IonSexp v = myFactory.newSexp(values);
        handle(v);
        return v;
    }

    public IonSexp newSexp(int[] values)
    {
        IonSexp v = myFactory.newSexp(values);
        handle(v);
        return v;
    }

    public IonSexp newSexp(long[] values)
    {
        IonSexp v = myFactory.newSexp(values);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonString newNullString()
    {
        IonString v = myFactory.newNullString();
        handle(v);
        return v;
    }

    public IonString newString(String value)
    {
        IonString v = myFactory.newString(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonStruct newNullStruct()
    {
        IonStruct v = myFactory.newNullStruct();
        handle(v);
        return v;
    }

    public IonStruct newEmptyStruct()
    {
        IonStruct v = myFactory.newEmptyStruct();
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonSymbol newNullSymbol()
    {
        IonSymbol v = myFactory.newNullSymbol();
        handle(v);
        return v;
    }

    public IonSymbol newSymbol(String value)
    {
        IonSymbol v = myFactory.newSymbol(value);
        handle(v);
        return v;
    }

    public IonSymbol newSymbol(SymbolToken value)
    {
        IonSymbol v = myFactory.newSymbol(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public IonTimestamp newNullTimestamp()
    {
        IonTimestamp v = myFactory.newNullTimestamp();
        handle(v);
        return v;
    }

    public IonTimestamp newTimestamp(Timestamp value)
    {
        IonTimestamp v = myFactory.newTimestamp(value);
        handle(v);
        return v;
    }

    //-------------------------------------------------------------------------

    public <T extends IonValue> T clone(T value)
        throws IonException
    {
        T v = myFactory.clone(value);
        handle(v);
        return v;
    }
}
