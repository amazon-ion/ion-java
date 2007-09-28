/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.Reader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Implements the Ion <code>struct</code> type.
 */
public final class IonStructImpl
    extends IonContainerImpl
    implements IonStruct
{

    private static final int NULL_STRUCT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidStruct,
                                        IonConstants.lnIsNullStruct);

    static final int ORDERED_STRUCT_TYPEDESC =
        IonConstants.makeTypeDescriptor(IonConstants.tidStruct,
                                        IonConstants.lnIsOrderedStruct);


    private boolean _isOrdered = false;


    /**
     * Constructs a <code>null.struct</code> value.
     */
    public IonStructImpl()
    {
        this(NULL_STRUCT_TYPEDESC);
        _hasNativeValue = true;
    }

    /**
     * Constructs a binary-backed struct value.
     */
    public IonStructImpl(int typeDesc)
    {
        super(typeDesc);
        assert pos_getType() == IonConstants.tidStruct;
    }


    @Override
    public boolean isNullValue()
    {
        if (_hasNativeValue || !_isPositionLoaded) {
            return (_contents == null);
        }

        int ln = this.pos_getLowNibble();
        return (ln == IonConstants.lnIsNullStruct);
    }


    public IonValue get(IonSymbol fieldName)
    {
        makeReady();
        return get(fieldName.stringValue());
    }

    public IonValue get(String fieldName)
    {
        validateFieldName(fieldName);
        validateThisNotNull();

        makeReady();

        for (IonValue field : this) {
            if (fieldName.equals(field.getFieldName())) {
                return field;
            }
        }
        return null;
    }

//    public IonValueImpl findField(String name, IonValueImpl prev)
//    {
//        makeReady();
//
//        int sid = this.getSymbolTable().findSymbol(name);
//        for (IonValue v : this) {
//            IonValueImpl vi = (IonValueImpl)v;
//            if (vi._fieldSid == sid) return vi;
//        }
//
//        return null;
//    }

    public void put(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        validateFieldName(fieldName);
        if (value == null) {
            throw new NullPointerException("value must not be null");
        }

        makeReady();

        if (_contents != null) {
            try {
                for (Iterator i = _contents.iterator(); i.hasNext();)
                {
                    IonValue child = (IonValue) i.next();
                    if (fieldName.equals(child.getFieldName()))
                    {
                        i.remove();
                        ((IonValueImpl)child).detachFromContainer();
                    }
                }
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }

        IonValueImpl concrete = (IonValueImpl) value;
        add(concrete);

        // This should be true because we've validated that its not contained.
        assert value.getFieldName() == null;
        concrete.setFieldName(fieldName);
    }


    public void add(String fieldName, IonValue value)
    {
        // TODO maintain _isOrdered

        validateFieldName(fieldName);
        if (value == null) {
            throw new NullPointerException("value must not be null");
        }

        IonValueImpl concrete = (IonValueImpl) value;
        add(concrete);

        // This should be true because we've validated that its not contained.
        assert value.getFieldName() == null;
        concrete.setFieldName(fieldName);
    }

    /**
     * Ensures that a given field name is valid. Used as a helper for
     * methods that have that precondition.
     *
     * @throws IllegalArgumentException if <code>fieldName</code> is empty.
     * @throws NullPointerException if the <code>fieldName</code>
     * is <code>null</code>.
     */
    private static void validateFieldName(String fieldName)
    {
        if (fieldName.length() == 0)
        {
            throw new IllegalArgumentException("fieldName must not be empty");
        }
    }


    public void accept(ValueVisitor visitor) throws Exception
    {
        makeReady();
        visitor.visit(this);
    }


    //=========================================================================
    // Helpers

    @Override
    public int getTypeDescriptorAndLengthOverhead(int contentLength)
    {
        int len = IonConstants.BB_TOKEN_LEN;

        if (isNullValue() || isEmpty())
        {
            assert contentLength == 0;
            return len;
        }

        if (_isOrdered || contentLength >= IonConstants.lnIsVarLen)
        {
            len += IonBinary.lenVarUInt7(contentLength);
        }

        return len;
    }

    @Override
    protected int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue;

        if (_contents == null) return IonConstants.lnIsNullStruct;

        if (_contents.isEmpty()) return IonConstants.lnIsEmptyContainer;

        if (_isOrdered) return IonConstants.lnIsOrderedStruct;

        int contentLength = this.getNativeValueLength();  // FIXME check nakedLength?
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

        writer.write(this.pos_getTypeDescriptorByte());

        if (_contents != null && _contents.size() > 0)
        {
            // TODO Rewrite this to avoid computing length again
            int vlen = this.getNativeValueLength();

            if (_isOrdered || vlen >= IonConstants.lnIsVarLen)
            {
                writer.writeVarUInt7Value(vlen, true);
            }

            cumulativePositionDelta =
                doWriteContainerContents(writer, cumulativePositionDelta);
        }

        return cumulativePositionDelta;
    }

    @Override
    public void updateSymbolTable(LocalSymbolTable symtab) {
        super.updateSymbolTable(symtab);
        if (this._contents != null) {
            for (IonValue v : this._contents) {
                symtab.addSymbol(v.getFieldName());
            }
        }
    }


    /**
     * Overrides IonValueImpl.container copy to add code to
     * read the field name symbol ids (fieldSid)
     * @throws IOException
     */
    @Override
    protected void doMaterializeValue(Reader reader) throws IOException
    {
        IonValueImpl            child;
        LocalSymbolTable        symtab = this.getSymbolTable();
        IonBinary.BufferManager buffer = this._buffer;

        int pos = this.pos_getOffsetAtActualValue();
        int end = this.pos_getOffsetofNextValue();

        // TODO compute _isOrdered flag

        while (pos < end) {
            reader.setPosition(pos);
            int sid = reader.readVarUInt7IntValue();
            child = makeValueFromReader(sid, reader, buffer, symtab, this);
            _contents.add(child);
            pos = child.pos_getOffsetofNextValue();
        }
        assert pos == end;  // TODO should throw IonException

    }
}
