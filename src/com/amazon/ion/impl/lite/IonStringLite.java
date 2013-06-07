// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

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

    /**
     * makes a copy of this IonString. This calls up to
     * IonTextImpl to copy the string itself and that in
     * turn calls IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * The symbol table is not copied as the value is fully
     * materialized and the symbol table is unnecessary.
     */
    @Override
    public IonStringLite clone()
    {
        IonStringLite clone = new IonStringLite(this._context.getSystem(), false);

        clone.copyFrom(this);

        return clone;
    }

    /**
     * Implements {@link Object#hashCode()} consistent with equals. This
     * implementation uses the hash of the string value XOR'ed with a constant.
     *
     * @return  An int, consistent with the contracts for
     *          {@link Object#hashCode()} and {@link Object#equals(Object)}.
     */
    @Override
    public int hashCode() {
        int hash = HASH_SIGNATURE;
        if (!isNullValue())  {
            hash ^= stringValue().hashCode();
        }
        return hash;
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
