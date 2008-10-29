/* Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion.impl;

import com.amazon.ion.IonText;

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
    protected IonTextImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
    }


    @Override
    public abstract IonTextImpl clone();


    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * @param source instance to copy from
     */
    protected final void copyFrom(IonTextImpl source)
    {
        // first copy the annotations and such, which
        // will materialize the value as needed.
        copyAnnotationsFrom(source);

        // now we can copy the text as a string
        String s = source._text_value;
        _set_value(s);
    }


    public void setValue(String value)
    {
        checkForLock();
        _set_value(value);
        setDirty();
    }

    /** Must call {@link #makeReady()} before calling. */
    protected final String _get_value()
    {
        return _text_value;
    }

    /**
     * Must call {@link #checkForLock()} first.
     * Also sets {@link #_hasNativeValue} true.
     */
    protected final void _set_value(String value)
    {
        _text_value = value;
        _hasNativeValue = true;
    }

    @Override
    public final boolean isNullValue()
    {
        if (!_hasNativeValue) return super.isNullValue();
        return (_text_value == null);
    }

}
