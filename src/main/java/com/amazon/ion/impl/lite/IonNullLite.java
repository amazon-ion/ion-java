// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.lite;

import com.amazon.ion.IonNull;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;


final class IonNullLite
    extends IonValueLite
    implements IonNull
{
    private static final int HASH_SIGNATURE =
        IonType.NULL.toString().hashCode();

    protected IonNullLite(ContainerlessContext context)
    {
        super(context, true);
    }

    IonNullLite(IonNullLite existing, IonContext context)
    {
        super(existing, context);
    }

    @Override
    public void accept(final ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    @Override
    IonValueLite shallowClone(IonContext context)
    {
        return new IonNullLite(this, context);
    }

    @Override
    public IonNullLite clone()
    {
        return (IonNullLite) shallowClone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    public IonType getTypeSlow()
    {
        return IonType.NULL;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeNull();
    }

    @Override
    int hashSignature() {
        return HASH_SIGNATURE;
    }

    @Override
    public int scalarHashCode() {
        return hashTypeAnnotations(HASH_SIGNATURE);
    }

}
