/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl.lite;

import software.amazon.ion.IonText;

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
