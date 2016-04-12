// Copyright (c) 2010-2015 Amazon.com, Inc.  All rights reserved.

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
    IonNullLite clone(IonContext context)
    {
        return new IonNullLite(this, context);
    }

    @Override
    public IonNullLite clone()
    {
        return clone(ContainerlessContext.wrap(getSystem()));
    }

    @Override
    public IonType getType()
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
    public int hashCode(SymbolTableProvider symbolTableProvider) {
        return hashTypeAnnotations(HASH_SIGNATURE, symbolTableProvider);
    }

}
