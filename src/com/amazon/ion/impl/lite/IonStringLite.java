// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonException;
import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;

/**
 *
 */
final class IonStringLite
    extends IonTextLite
    implements IonString
{
    private static final int HASH_SIGNATURE =
        IonType.STRING.toString().hashCode();

    /**
     * Constructs a <code>null.string</code> value.
     */
    public IonStringLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }

    @Override
    public IonStringLite clone()
    {
        IonStringLite clone = new IonStringLite(this._context.getSystem(), false);

        // Copy relevant member fields and text value
        clone.copyFrom(this);

        return clone;
    }

    @Override
    public int hashCode()
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue()) {
            result ^= stringValue().hashCode();
        }

        return hashTypeAnnotations(result);
    }

    @Override
    public IonType getType()
    {
        return IonType.STRING;
    }

    public final void writeTo(IonWriter writer) {
        try {
            writer.setTypeAnnotationSymbols(getTypeAnnotationSymbols());
            if (isNullValue()) {
                writer.writeNull(IonType.STRING);
            } else {
                writer.writeString(stringValue());
            }
        } catch (Exception e) {
            throw new IonException(e);
        }
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
