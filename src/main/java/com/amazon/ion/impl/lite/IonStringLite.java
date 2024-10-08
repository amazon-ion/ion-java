// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.lite;

import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


final class IonStringLite
    extends IonTextLite
    implements IonString
{
    private static final int HASH_SIGNATURE =
        IonType.STRING.toString().hashCode();

    /**
     * Constructs a <code>null.string</code> value.
     */
    IonStringLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonStringLite(IonStringLite existing, IonContext context)
    {
        super(existing, context);
        // no need to set values as these are set at the parent IonTextLite
    }

    @Override
    IonValueLite shallowClone(IonContext parentContext)
    {
        return new IonStringLite(this, parentContext);
    }

    @Override
    public IonStringLite clone()
    {
        return (IonStringLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    int scalarHashCode()
    {
        int result = HASH_SIGNATURE;
        result ^= _text_value.hashCode();

        return hashTypeAnnotations(result);
    }

    @Override
    public IonType getTypeSlow()
    {
        return IonType.STRING;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeString(_text_value);
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
