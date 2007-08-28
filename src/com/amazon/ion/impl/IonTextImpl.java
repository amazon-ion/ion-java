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


    public String getValue()
        throws NullValueException
    {
        makeReady();
        if (_text_value == null) throw new NullValueException();
        return _text_value;
    }

    public void setValue(String value)
    {
        _text_value = value;
        _hasNativeValue = true;
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
    }

    @Override
    public synchronized boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_text_value == null);
    }

}
