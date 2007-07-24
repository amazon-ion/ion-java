/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

import com.amazon.ion.IonClob;
import com.amazon.ion.IonException;
import com.amazon.ion.ValueVisitor;


/**
 * Implements the Ion <code>clob</code> type.
 */
public final class IonClobImpl
    extends IonLobImpl
    implements IonClob
{
    
    static final int _clob_typeDesc = 
        IonConstants.makeTypeDescriptorByte(
                         IonConstants.tidClob
                        ,IonConstants.lnIsNullAtom
        );
    
    /**
     * Constructs a <code>null.clob</code> element.
     */
    public IonClobImpl()
    {
        super(_clob_typeDesc);
    }

    /**
     * Constructs a binary-backed element.
     */
    public IonClobImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidClob;
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
        
        // TODO use Charset directly.
        byte[] bytes = newBytes();
        if (bytes == null) return null;

        try
        {
            return new String(bytes, cs.name());
        }
        catch (UnsupportedEncodingException e)
        {
            // This shouldn't happen since the Charset already exits.
            throw new IonException("Unsupported encoding " + cs.name(), e);
        }
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }
}
