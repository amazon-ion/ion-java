// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonText;

abstract class IonTextLite
    extends IonValueLite
    implements IonText
{
    private String _text_value;

    protected IonTextLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonTextLite(IonTextLite existing, IonContext context)
    {
        super(existing, context);
        // String is immutable so can copy reference (including a null ref)
        this._text_value = existing._text_value;
    }

    @Override
    public abstract IonTextLite clone();

    public void setValue(String value)
    {
        checkForLock();
        _set_value(value);
    }

    /**
     * @return null iff {@link #isNullValue()}
     */
    protected final String _get_value()
    {
        return _text_value;
    }

    public String stringValue()
    {
        return _text_value;
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
