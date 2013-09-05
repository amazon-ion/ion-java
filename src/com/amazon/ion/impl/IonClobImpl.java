// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_IonConstants.lnIsNullAtom;
import static com.amazon.ion.impl._Private_IonConstants.makeTypeDescriptor;
import static com.amazon.ion.impl._Private_IonConstants.tidClob;

import com.amazon.ion.IonClob;
import com.amazon.ion.IonType;
import com.amazon.ion.ValueVisitor;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;


/**
 * Implements the Ion <code>clob</code> type.
 */
final class IonClobImpl
    extends IonLobImpl
    implements IonClob
{

    static final int NULL_CLOB_TYPEDESC =
        makeTypeDescriptor(tidClob, lnIsNullAtom);

    static private final int HASH_SIGNATURE =
        IonType.CLOB.toString().hashCode();

    /**
     * Constructs a <code>null.clob</code> element.
     */
    public IonClobImpl(IonSystemImpl system)
    {
        super(system, NULL_CLOB_TYPEDESC);
        _hasNativeValue(true); // Since this is null
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonClobImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
        assert pos_getType() == _Private_IonConstants.tidClob;
    }

    /**
     * makes a copy of this IonClob including an independant
     * copy of the bytes. It also calls IonValueImpl to copy
     * the annotations and the field name if appropriate.
     * The symbol table is not copied as the value is fully
     * materialized and the symbol table is unnecessary.
     */
    @Override
    public IonClobImpl clone()
    {
        IonClobImpl clone = new IonClobImpl(_system);

        clone.copyFrom(this);

        return clone;
    }

    @Override
    public int hashCode() {
        return lobHashCode(HASH_SIGNATURE);
    }

    public IonType getType()
    {
        return IonType.CLOB;
    }


    public Reader newReader(Charset cs)
    {
        InputStream in = newInputStream();
        if (in == null) return null;

        makeReady();
        return new InputStreamReader(in, cs);
    }


    public String stringValue(Charset cs)
    {
        makeReady();

        byte[] bytes = getBytes();
        if (bytes == null) return null;

        return _Private_Utils.decode(bytes, cs);
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
