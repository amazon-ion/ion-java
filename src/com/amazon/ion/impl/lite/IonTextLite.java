// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

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

    protected IonTextLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public abstract IonTextLite clone();

    /**
     * This copies the original value's member fields (flags, annotations,
     * context) from the passed in {@code original} instance and overwrites
     * the corresponding fields of this instance. It also copies the text value.
     * Since only string representations are copied, it is unnecessary to
     * update the symbol table.. yet.
     *
     * @param original
     */
    protected final void copyFrom(IonTextLite original)
    {
        // first copy the annotations and such, which
        // will materialize the value as needed
        this.copyMemberFieldsFrom(original);

        // now we can copy the text value as a string
        String s = original._text_value;
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
