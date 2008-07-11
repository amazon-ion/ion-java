/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonText;
import com.amazon.ion.NullValueException;

/**
 *
 */
abstract class IonTextImpl
    extends IonValueImpl
    implements IonText
{
    private String _text_value;

    /**
     * Constructs a binary-backed value.
     */
    protected IonTextImpl(int typeDesc)
     {
         super(typeDesc);
     }

    /**
     * throws a CloneNotSupportedException as this is a
     * parent type that should not be directly created.
     * Instances should constructed of either IonStringImpl
     * or IonSymbolImpl as needed.
     */
    public IonTextImpl clone() throws CloneNotSupportedException
    {
    	throw new CloneNotSupportedException();
    }
    
    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * @param source instance to copy from
     */
    protected void copyFrom(IonTextImpl source)
    {
    	// first copy the annotations and such, which
    	// will materialize the value as needed.
    	super.copyFrom(source);

    	// now we can copy the text as a string
    	String s = source.getValue();
    	_set_value(s);
    }


    public String getValue()
        throws NullValueException
    {
        makeReady();
        if (_text_value == null) throw new NullValueException();
        return _text_value;
    }

    public void setValue(String value)
    {
    	checkForLock();
    	_set_value(value);
    	setDirty();
    }

    /** Must call {@link #makeReady()} before calling. */
    protected String _get_value()
    {
        return _text_value;
    }

    protected void _set_value(String value)
    {
        _text_value = value;
        _hasNativeValue = true;
    }

    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_text_value == null);
    }

}
