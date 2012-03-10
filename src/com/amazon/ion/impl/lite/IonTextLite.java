// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonText;

/**
 *
 */
abstract class IonTextLite
    extends IonValueLite
    implements IonText
{
    private String _text_value;

    /**
     * Constructs a binary-backed value.
     */
    protected IonTextLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public abstract IonTextLite clone();


    /**
     * this copies the annotations and the field name if
     * either of these exists from the passed in instance.
     * It overwrites these values on the current instance.
     * Since these will be the string representations it
     * is unnecessary to update the symbol table ... yet.
     * @param source instance to copy from
     */
    protected final void copyFrom(IonTextLite source)
    {
        // first copy the annotations and such, which
        // will materialize the value as needed.
        this.copyValueContentFrom(source);

        // now we can copy the text as a string
        String s = source._text_value;
        _set_value(s);
    }

    public void setValue(String value)
    {
        checkForLock();
        _set_value(value);
    }

    protected final String _get_value()
    {
        return _text_value;
    }

    public String stringValue()
    {
        if (isNullValue()) {
            return null;
        }
        return _get_value();
    }

    /**
     * Must call {@link #checkForLock()} first.
     */
    protected final void _set_value(String value)
    {
        _text_value = value;
        _isNullValue(value == null);
    }

}
