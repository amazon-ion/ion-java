// Copyright (c) 2010-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonNull;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
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

    @Override
    public void accept(final ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

    @Override
    public IonNullLite clone()
    {
        IonNullLite clone = new IonNullLite(_context.getSystem());

        // As IonNulls have no value, we only need to copy member fields
        clone.copyMemberFieldsFrom(this);

        return clone;
    }

    @Override
    public IonType getType()
    {
        return IonType.NULL;
    }

    @Override
    final void writeBodyTo(IonWriter writer)
        throws IOException
    {
        writer.writeNull();
    }

    @Override
    public int hashCode() {
        return hashTypeAnnotations(HASH_SIGNATURE);
    }

}
