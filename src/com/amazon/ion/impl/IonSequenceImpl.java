/*
 * Copyright (c) 2007 Amazon.com, Inc. All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.SymbolTable;
import java.io.IOException;
import java.util.ArrayList;


/**
 * Base class for list and sexp implementations.
 */
public abstract class IonSequenceImpl
    extends IonContainerImpl
    implements IonSequence
{

    /**
     * Constructs a sequence backed by a binary buffer.
     *
     * @param typeDesc
     */
    protected IonSequenceImpl(int typeDesc)
    {
        super(typeDesc);
        assert !_hasNativeValue;
    }

    /**
     * Constructs a sequence value <em>not</em> backed by binary.
     *
     * @param typeDesc
     * @param makeNull
     */
    protected IonSequenceImpl(int typeDesc, boolean makeNull)
    {
        this(typeDesc);
        assert _contents == null;
        assert isDirty();

        if (!makeNull)
        {
            _contents = new ArrayList<IonValue>();
        }
        _hasNativeValue = true;
    }

    @Override
    public boolean isNullValue()
    {
        if (_hasNativeValue || !_isPositionLoaded) {
            return (_contents == null);
        }

        int ln = this.pos_getLowNibble();
        return (ln == IonConstants.lnIsNullSequence);
    }

    @Override
    // Increasing visibility
    public void add(IonValue element)
        throws ContainedValueException, NullPointerException
    {
        super.add(element);
    }

    @Override
    // Increasing visibility
    public void add(int index, IonValue element)
        throws ContainedValueException, NullPointerException
    {
        super.add(index, element);
    }

    public void addEmbedded(IonValue element)
        throws NullPointerException
    {
        LocalSymbolTable symtab = element.getSymbolTable();

        IonSexpImpl wrapper = new IonSexpImpl();
        wrapper.addTypeAnnotation(SymbolTable.ION_EMBEDDED_VALUE);

        String systemId = symtab.getSystemSymbolTable().getSystemId();
        // TODO inject systemId ($ion_1_0)
        // TODO inject symtab
        // TODO inject value

        assert wrapper._isSystemValue; // so we can unwrap it
        super.add(wrapper);
    }

    @Override
    protected int computeLowNibble(int valuelen)
        throws IOException
    {
        assert _hasNativeValue;

        if (_contents == null) { return IonConstants.lnIsNullSequence; }

        int contentLength = getNakedValueLength();
        if (contentLength > IonConstants.lnIsVarLen)
        {
            return IonConstants.lnIsVarLen;
        }

        return contentLength;
    }

    @Override
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta)
        throws IOException
    {
        assert _hasNativeValue == true || _isPositionLoaded == false;
        assert !(this instanceof IonDatagram);

        writer.write(this.pos_getTypeDescriptorByte());

        // now we write any data bytes - unless it's null
        int vlen = this.getNativeValueLength();
        if (vlen > 0)
        {
            if (vlen >= IonConstants.lnIsVarLen)
            {
                writer.writeVarUInt7Value(vlen, true);
                // Fall through...
            }

            // TODO cleanup; this is the only line different from super
            cumulativePositionDelta =
                doWriteContainerContents(writer,
                                         cumulativePositionDelta);
        }
        return cumulativePositionDelta;
    }
}
