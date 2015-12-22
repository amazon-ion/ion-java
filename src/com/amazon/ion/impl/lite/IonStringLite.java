// Copyright (c) 2010-2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonString;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ValueVisitor;
import java.io.IOException;

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
    public IonStringLite(IonContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonStringLite(IonStringLite existing, IonContext context)
    {
        super(existing, context);
        // no need to set values as these are set at the parent IonTextLite
    }

    @Override
    IonStringLite clone(IonContext parentContext)
    {
        return new IonStringLite(this, parentContext);
    }

    @Override
    public IonStringLite clone()
    {
        return clone(StubContext.wrap(getSystem()));
    }

    @Override
    int hashCode(SymbolTableProvider symbolTableProvider)
    {
        int result = HASH_SIGNATURE;

        if (!isNullValue()) {
            result ^= stringValue().hashCode();
        }

        return hashTypeAnnotations(result, symbolTableProvider);
    }

    @Override
    public IonType getType()
    {
        return IonType.STRING;
    }

    @Override
    final void writeBodyTo(IonWriter writer, SymbolTableProvider symbolTableProvider)
        throws IOException
    {
        writer.writeString(_get_value());
    }

    @Override
    public void accept(ValueVisitor visitor) throws Exception
    {
        visitor.visit(this);
    }

}
