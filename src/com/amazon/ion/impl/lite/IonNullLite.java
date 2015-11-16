// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonNull;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;

/**
 *
 */
final class IonNullLite
    extends IonValueLite
    implements IonNull
{
    private static final int HASH_SIGNATURE =
        IonType.NULL.toString().hashCode();

    protected IonNullLite(IonContext context)
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
        return clearFieldName(this.clone(getSystem()));
    }

    @Override
    public IonType getType()
    {
        return IonType.NULL;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTable symbolTable)
        throws IOException
    {
        writer.writeNull();
    }

    @Override
    public int hashCode(SymbolTable symbolTable) {
        return hashTypeAnnotations(HASH_SIGNATURE, symbolTable);
    }

}
