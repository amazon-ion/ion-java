/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.impl;

import com.amazon.ion.ContainedValueException;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonList;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import com.amazon.ion.LocalSymbolTable;
import com.amazon.ion.NullValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.ValueVisitor;
import com.amazon.ion.impl.IonBinary.BufferManager;
import com.amazon.ion.impl.IonBinary.Reader;
import com.amazon.ion.impl.IonBinary.Writer;
import com.amazon.ion.util.Printer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 *
 */
public abstract class IonValueImpl
    implements IonValue
{

    /**
     * We could mulitplex this with member id, but it adds way more complexity
     * than it saves space.
     */
    int         _elementid;
    private String _fieldName;
    String[]    _annotations;

    //
    //      | td w vlen | value |
    //      | td | vlen | value |
    //      | an w tlen | alen | ann | td w vlen | value |
    //      | an w tlen | alen | ann | td | vlen | value |
    //      | an | tlen | alen | ann | td w vlen | value |
    //      | an | tlen | alen | ann | td | vlen | value |
    //
    //      | mid | td w vlen | value |
    //      | mid | td | vlen | value |
    //      | mid | an w tlen | alen | ann | td w vlen | value |
    //      | mid | an w tlen | alen | ann | td | vlen | value |
    //      | mid | an | tlen | alen | ann | td w vlen | value |
    //      | mid | an | tlen | alen | ann | td | vlen | value |
    //      |     |                        |                   |
    //       ^ entry_start                  ^ value_td_start
    //             ^ [+ len(mid)]   value_content_start ^
    //                                               next_start ^
    //
    protected int _fieldSid;        // field symbol id in buffer

    /**
     * The actual TD byte of the value.
     * TODO document when the low-nibble is known to be correct.
     */
    private int _type_desc;

    private int _entry_start;       // offset of initial td, possibly an annotation
    private int _value_td_start;    // offset of td of the actual value
    private int _value_content_start;// offset of td of the actual value

    // TODO should be private but I needed to change it in DatagramImpl ctor
    protected int _next_start;        // offset of the next value (end + 1)

    /**
     * states for a value:
     *
     * materialized - value has been loaded from the buffer into the native
     *                  value, positionLoaded is a precondition for a value
     *                  to be materialized. This is false if there is no buffer.
     *
     * positionLoaded - the type descriptor information has been loaded from
     *                  the buffer.  This is always true if the value is
     *                  materialized.  And always false if there is no buffer.
     *
     * hasNativeValue - the Java value has been set, either directly or
     *                  through materialization.
     *
     * isDirty - the value in the buffer does not match the value in the
     *                  native reference.  This is always true when there is
     *                  no buffer.
     */

    /**
     * Indicates whether or not a value has been loaded from its buffer.
     * If the value does not have backing buffer this is always false;
     * If this value is true the _isPositionLoaded must also be true;
     */
    protected boolean      _isMaterialized;

    /**
     * Indicates whether or not the position information has been loaded.
     * If the value does not have backing buffer this is always false;
     */
    protected boolean      _isPositionLoaded;

    /**
     * Indicates whether our nativeValue (stored by concrete subclasses)
     * has been determined.
     */
    protected boolean      _hasNativeValue;

    /**
     * Indicates whether our {@link #_nativeValue} has been updated and is
     * is currently out of synch with the underlying byte buffer. Initially
     * a non-buffer backed value is dirty, and a buffer backed value (even
     * if it has not been materialized) is clean.  Don't modify this directly,
     * go through {@link #setDirty()}.  If this flag is true, then our parent
     * must be dirty, too.  If this flag is true, we {@link #_hasNativeValue}
     * must be true too.
     * <p>
     * Note that a value with no buffer is always dirty.
     */
    private boolean      _isDirty;


    boolean              _isSystemValue;

    /**
     * This is the containing value, if there is one.  The container
     * may hold the symbol table and/or the buffer.  If the container
     * changes the buffer and symbol table need to be checked for
     * equality.  If these change then the value will need to be
     * materialized before the change is made as the "old" buffer or
     * symbol table may be needed for the value to be understood.
     */
    protected IonValueImpl.container _container;

    /**
     * The buffer with this element's binary-encoded data.  May be non-null
     * even when {@link #_container} is null.
     */
    BufferManager       _buffer;

    /**
     * The local symbol table is an updatable (?) copy of the
     * symbol table, if one exists for this value.  This may be
     * stored in at a parent container.
     */
    protected LocalSymbolTable _symboltable;


    /**
     * Constructs a value with the given native value and high-nibble, and zero
     * low-nibble.
     */
    protected IonValueImpl(int typedesc)
    {
        pos_init();
        _isMaterialized     = false;
        _isPositionLoaded   = false;
        _hasNativeValue     = false;
        _isDirty            = true;
        pos_setTypeDescriptorByte(typedesc);
    }

    protected void init(int fieldSID
                       ,BufferManager buffer
                       ,int offset
                       ,IonValueImpl.container container
                       ,LocalSymbolTable symboltable
                       )
    {
        assert symboltable != null;

        _fieldSid    = fieldSID;
        _buffer      = buffer;
        _container   = container;
        _symboltable = symboltable;

        try {
            IonBinary.Reader reader = buffer.reader();
            reader.sync();
            reader.setPosition(offset);
            pos_load(fieldSID, reader);
        }
        catch (IOException e) {
            throw new IonException(e);
        }

    }

    protected void makeReady()
    {
        if (_hasNativeValue) return;
        if (_isMaterialized) return;
        if (_entry_start != -1) {
            assert _isPositionLoaded == true;
            try {
                materialize();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }
    }

    /**
     * @return not null.
     */
    public static IonValueImpl makeValueFromBuffer(
                                     int fieldSID
                                    ,int position
                                    ,BufferManager buffer
                                    ,LocalSymbolTable symboltable
                                    ,IonValueImpl.container container
    ) {
        IonValueImpl value;
        assert symboltable != null;

        try {
            IonBinary.Reader reader = buffer.reader();
            reader.sync();
            reader.setPosition(position);
            value = makeValueFromReader(fieldSID
                                       ,reader
                                       ,buffer
                                       ,symboltable
                                       ,container
                    );
        }
        catch (IOException e) {
            throw new IonException(e);
        }

        return value;
    }

    /**
     * @return not null.
     */
    public static IonValueImpl makeValueFromReader(
                                       int fieldSID
                                      ,IonBinary.Reader reader
                                      ,BufferManager buffer
                                      ,LocalSymbolTable symboltable
                                      ,IonValueImpl.container container
    )
        throws IOException
    {
        IonValueImpl value;
        assert symboltable != null;

        int pos = reader.position();
        int tdb = reader.readActualTypeDesc();
        value = makeValue(tdb);
        value.init(fieldSID
                  ,buffer
                  ,pos
                  ,container
                  ,symboltable
        );

        return value;
    }

    static IonValueImpl makeValue(int typedesc)
    {
        IonValueImpl value = null;
        int typeId = IonConstants.getTypeDescriptor(typedesc);

        switch (typeId) {
        case IonConstants.tidNull: // null(0)
            value = new IonNullImpl(typedesc);
            break;
        case IonConstants.tidBoolean: // boolean(1)
            value = new IonBoolImpl(typedesc);
            break;
        case IonConstants.tidPosInt: // 2
        case IonConstants.tidNegInt: // 3
            value = new IonIntImpl(typedesc);
            break;
        case IonConstants.tidFloat: // float(4)
            value = new IonFloatImpl(typedesc);
            break;
        case IonConstants.tidDecimal: // decimal(5)
            value = new IonDecimalImpl(typedesc);
            break;
        case IonConstants.tidTimestamp: // timestamp(6)
            value = new IonTimestampImpl(typedesc);
            break;
        case IonConstants.tidSymbol: // symbol(7)
            value = new IonSymbolImpl(typedesc);
            break;
        case IonConstants.tidString: // string (8)
            value = new IonStringImpl(typedesc);
            break;
        case IonConstants.tidClob: // clob(9)
            value = new IonClobImpl(typedesc);
            break;
        case IonConstants.tidBlob: // blob(10)
            value = new IonBlobImpl(typedesc);
            break;
        case IonConstants.tidList: // list(11)
            value = new IonListImpl(typedesc);
            break;
        case IonConstants.tidSexp: // 12
            value = new IonSexpImpl(typedesc);
            break;
        case IonConstants.tidStruct: // 13
            value = new IonStructImpl(typedesc);
            break;

        case IonConstants.tidTypedecl: // 14
        default:
            throw new IonException("invalid type "+typeId+" ("+typedesc+") encountered");
        }

        return value;
    }


    public synchronized boolean isNullValue()
    {
        // container overrides this method
        assert ! (this instanceof IonContainer);

        // scalar classes override this, but call the
        // super() copy (this one) when they don't
        // have a native value.
        assert _hasNativeValue == false || _isPositionLoaded == true;

        int ln = this.pos_getLowNibble();
        return (ln == IonConstants.lnIsNullAtom);
    }


    /**
     * Ensures that this value is not an Ion null.  Used as a helper for
     * methods that have that precondition.
     * @throws NullValueException if <code>this.isNullValue()</code>
     */
    protected final void validateThisNotNull()
        throws NullValueException
    {
        if (isNullValue())
        {
            throw new NullValueException();
        }
    }

    private final boolean deservesEmbeddingWithLocalSymbolTable() {
        // TODO: this is a stub
        return false;
    }

    public String getFieldName()
    {
        if (this._fieldName != null) return this._fieldName;
        if (this._fieldSid == 0) return null;
        _fieldName = this.getSymbolTable().findSymbol(this._fieldSid);
        assert _fieldName != null;
        return _fieldName;
    }

    public int getFieldNameId()
    {
        if (this._fieldSid == 0 && this._fieldName != null)
        {
            this._fieldSid = getSymbolTable().addSymbol(this._fieldName);
            // TODO define behavior if there's no symbol table!
        }
        return this._fieldSid;
    }

    public final IonContainer getContainer()
    {
        if (_container == null) {
            return null;
        }
        if (_container._isSystemValue) {
            return _container.getContainer();
        }
        return _container;
    }


    public void doNothing() {
        // used for timing things (to simulate a method call, but not do any work)
    }

    public final boolean isDirty() {
        return _isDirty;
    }

    /**
     * Marks this element, and it's container, dirty.  This forces the binary
     * buffer to be updated when it's next needed.
     */
    protected void setDirty() {
        if (this._isDirty == false) {
            this._isDirty = true;
            if (this._container != null) {
                this._container.setDirty();
            }
        }
        else {
            assert this._container == null || this._container.isDirty();
        }
    }

    protected void setClean() {
        assert this.getBuffer() != null;
        this._isDirty = false;
    }

    public BufferManager getBuffer() {
        if (_buffer != null)    return _buffer;
        if (_container != null) return _container.getBuffer();
        return null;
    }

    // overridden for struct, which really needs to have a symbol table
    public LocalSymbolTable getSymbolTable() {
        if (this._symboltable != null)  return this._symboltable;
        if (this._container != null)    return this._container.getSymbolTable();
        return null;
    }

    public void setSymbolTable(LocalSymbolTable symtab) {
        this._symboltable = symtab;
    }

    public int getElementId() {
        return this._elementid;
    }

    /**
     * @deprecated Use {@link #getTypeAnnotations()} instead
     */
    public String[] getTypeAnnotationStrings()
    {
        return getTypeAnnotations();
    }

    public String[] getTypeAnnotations()
    {
        // if we have a list, then it's a good one
        if (this._annotations != null) return this._annotations;

        // if this has been materialized (and we clearly don't
        // have a list) then there is no annotations
        makeReady();

        return this._annotations;
    }

    public void clearTypeAnnotations()
    {
        makeReady();
        this._annotations = null;
        this.setDirty();
    }

    public boolean hasTypeAnnotation(String annotation)
    {
        makeReady();
        if (_annotations != null) {
            for (String s : _annotations) {
                if (s.equals(annotation)) return true;
            }
        }
        return false;
    }

    public void removeTypeAnnotation(String annotation)
    {
        if (!this.hasTypeAnnotation(annotation)) return;

        String[] temp = (_annotations.length == 1) ? null : new String[_annotations.length - 1];
        for (int ii=0, jj=0; ii < _annotations.length; ii++) {
            if (!_annotations[ii].equals(annotation)) {
                temp[jj++] = _annotations[ii];
            }
        }
        _annotations = temp;

        return;
    }
    public void addTypeAnnotation(String annotation)
    {
        if (hasTypeAnnotation(annotation)) return;

        // allocate a larger array and copy if necessary
        int len = (_annotations == null) ? 0 : _annotations.length;
        String[] temp = new String[len + 1];
        if (_annotations != null) {
            System.arraycopy(this._annotations, 0, temp, 0, len);
        }
        // load the new sid
        temp[len] = annotation;
        this._annotations = temp;

        setDirty();
    }

    void pos_init()
    {
        _fieldSid           =  0;
        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;

    }
    void pos_init(int fieldNameSymbol)
    {
        pos_init();
        pos_setFieldId(fieldNameSymbol);
    }
    void pos_init(int fieldNameSymbol, Reader valueReader) throws IOException
    {
        pos_init(fieldNameSymbol);
        pos_load(fieldNameSymbol, valueReader);
    }

    // overridden in container to call for clearing the children
    // and the container version calls this to clear its local values
    void clear_position_and_buffer()
    {
        // these calls force this ion value to be fully reified
        makeReady();
        this.getFieldName();
        this.getTypeAnnotations();

        _buffer = null;
        _symboltable = null;               // ----------------------- CAS ADDED
        _isMaterialized     = false;
        _isPositionLoaded   = false;
        _isDirty            = true;

        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;
    }

    void pos_initDatagram(int typeDesc, int length) {
        _isMaterialized     = false;
        _isPositionLoaded   = true;
        _hasNativeValue     = false;

        // if we have a buffer, and it's not loaded, it has to be clean
        _isDirty            = false;

        _fieldSid           = 0;
        _type_desc          = typeDesc;
        _entry_start        = 8;
        _value_td_start     = 8;
        _value_content_start= 8;
        _next_start         = length;
    }

    void pos_clear()
    {
        _isMaterialized     = false;
        _isPositionLoaded   = false;
        _isDirty            = true;

        _fieldSid           =  0;
        _type_desc          =  0;
        _entry_start        = -1;
        _value_td_start     = -1;
        _value_content_start= -1;
        _next_start         = -1;
    }

    void pos_load(int fieldId, Reader valueReader) throws IOException
    {
        _isMaterialized     = false;
        _isPositionLoaded   = true;
        _hasNativeValue     = false;
        _isDirty            = false; // if we have a buffer, and it's not loaded, it has to be clean

        pos_setFieldId(fieldId);

        // we expect to be positioned at the initial typedesc byte
        int start;
        start = this._value_td_start = valueReader.position();

        // our _entry_start needs to be back filled to precede the field id
        this._entry_start = start - pos_getFieldIdLen();

        // read and remember our type descriptor byte
        this._type_desc = valueReader.readToken();

        // this first length is our overall length
        // whether we have annotations or not
        int type = this.pos_getType();
        int vlen = valueReader.readLength(type, this.pos_getLowNibble());

        this._value_content_start = valueReader.position();
        this._next_start = this._value_content_start + vlen;

        if (type == IonConstants.tidTypedecl) {
            // read past the annotation list
            int annotationListLength = valueReader.readVarUInt7IntValue();
            assert annotationListLength > 0;
            this._value_td_start = valueReader.position() + annotationListLength;
            valueReader.setPosition(this._value_td_start);
            // read the actual values type desc
            this._type_desc = valueReader.readToken();
            type = this.pos_getType();
            if ( type == IonConstants.tidStruct
              || this.pos_getLowNibble() == IonConstants.lnIsVarLen
            ) {
                valueReader.readVarUInt7IntValue();
            }
            this._value_content_start = valueReader.position();
        }
    }

    public final byte pos_getTypeDescriptorByte() {
        return (byte)(0xff & this._type_desc);
    }
    public final void pos_setTypeDescriptorByte(int td) {
        this._type_desc = (byte)(0xff & td);
    }
    public final int pos_getType() {
        return IonConstants.getTypeDescriptor(this._type_desc);
    }
    public final void pos_setType(int hn) {
        assert hn >= IonConstants.tidNull && hn <= IonConstants.tidUnused;
        IonConstants.makeTypeDescriptorByte(hn, this.pos_getLowNibble());
    }
    public final int pos_getLowNibble() {
        return IonConstants.getLowNibble(this._type_desc);
    }
    public final void pos_setLowNibble(int ln) {
        assert ln >= 0 && ln <= 0xf;
        IonConstants.makeTypeDescriptorByte(this.pos_getType(), ln);
    }

    public final int pos_getFieldId() {
        return _fieldSid;
    }
    public final int pos_getFieldIdLen() {
        return IonBinary.lenVarUInt7(_fieldSid);
    }
    public final void pos_setFieldId(int fieldNameSid)
    {
        assert fieldNameSid >= 0;
        int old_field_len = pos_getFieldIdLen();
        if (fieldNameSid == 0) {
            _fieldSid = 0;
        }
        else {
            _fieldSid = fieldNameSid;
        }
        int new_field_len = pos_getFieldIdLen();
        int delta = (new_field_len - old_field_len);
        // with more space taken by the field id, the actual
        // value and the next value must also move down
        _value_td_start += delta;
        _value_content_start += delta;
        _next_start  += delta;
    }

    public final int pos_getOffsetAtFieldId() {
        return this._entry_start;
    }
    public final int pos_getOffsetInitialTD() {
        return this._entry_start + this.pos_getFieldIdLen();
    }
    public final boolean pos_isAnnotated() {
        return (this._entry_start + IonBinary.lenVarUInt7(this._fieldSid) != this._value_td_start);
    }
    public final int pos_getOffsetAtAnnotationTD() {
        if (this.pos_isAnnotated()) {
            return this.pos_getOffsetInitialTD();
        }
        return -1;
    }
    public final int pos_getOffsetAtValueTD() {
        return this._value_td_start;
    }
    public final int pos_getOffsetAtActualValue() {
        return this._value_content_start;
    }
    public final int pos_getOffsetofNextValue() {
        return this._next_start;
    }
    public final int  pos_getValueOnlyLength() {
        return this._next_start - this._value_content_start;
    }

    public void pos_setEntryStart(int entryStart) {
        int delta = entryStart - this._entry_start;
        pos_moveAll(delta);
    }

    public void pos_moveAll(int delta) {
        this._entry_start += delta;
        this._value_td_start += delta;
        this._value_content_start += delta;
        this._next_start += delta;
    }

    /**
     * Gets the buffer for this element, set to the given position.
     * @param position
     * @return this element's buffer; can be null.
     * @throws IOException
     */
    IonBinary.Reader getReader(int position) throws IOException
    {
        BufferManager buf = this.getBuffer();
        if (buf == null) return null;
        return buf.reader(position);
    }
    IonBinary.Writer getWriter(int position) throws IOException
    {
        BufferManager buf = this.getBuffer();
        if (buf == null) return null;
        IonBinary.Writer writer = buf.writer();
        if (writer == null) return null;
        writer.setPosition(position);
        return writer;
    }


    static int getFieldLength(int td, IonBinary.Reader reader) throws IOException
    {
        int ln = IonConstants.getLowNibble(td);
        int hn = IonConstants.getTypeDescriptor(td);
        int len = reader.readLength(hn, ln);
        return len;
    }


    /**
     * Decodes the content of this element into Java values for access via
     * this object.  If this is a container, the children are not necessarily
     * materialized.
     * <p/>
     * Postcondition: <code>this._hasNativeValue == true </code>
     */
    protected synchronized void materialize() throws IOException
    {
        if ( this._isMaterialized ) return;

        if ( this._isPositionLoaded == false ) {
            if (this._buffer != null) {
                throw new IonException("invalid value state - buffer but not loaded!");
            }
            return;
        }

        if (this._buffer == null) {
            throw new IonException("invalid value state - loaded but no buffer!");
        }

        IonBinary.Reader reader = this.getBuffer().reader();
        reader.sync();
        if ( ! this.pos_isAnnotated() ) {
            // if there aren't annoations, just position the reader
            reader.setPosition(this.pos_getOffsetAtValueTD());
        }
        else {
            materializeAnnotations(reader);
        }

        // now materialize the value itself (this is done in the
        // specialized sub classes)
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // TODO doMaterializeValue should precondition !_hasNativeValue and
        // then set _hasNativeValue here, OnceAndOnlyOnce.
        this.doMaterializeValue(reader);
        assert _hasNativeValue;
        this._isMaterialized = true;
    }

    protected synchronized void materializeAnnotations(IonBinary.Reader reader) throws IOException
    {
        assert this._isMaterialized == false;

        if (!this.pos_isAnnotated()) return;

        //  so we read any annotations that are present
        reader.setPosition(this.pos_getOffsetAtAnnotationTD());

        // skip over the annoation td and total length
        int td = reader.read();
        if (IonConstants.getTypeDescriptor(td) != IonConstants.tidTypedecl) {
            throw new IonException("invalid user type annotation");
        }
        if (IonConstants.getLowNibble(td) == IonConstants.lnIsVarLen) {
            // skip past the overall length, which we don't care about here
            reader.readVarInt7IntValue();
        }

        // now we load the annotation SID list from the buffer
        int[] sids = reader.readAnnotations();
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // and convert them to strings
        int len = sids.length;
        this._annotations = new String[len];
        LocalSymbolTable symtab = getSymbolTable();
        for (int ii=0; ii<len; ii++) {
            int id = sids[ii];
            this._annotations[ii] = symtab.findSymbol(id);
        }
    }

    /**
     * Postcondition: <code>this._hasNativeValue == true</code>
     *
     * @param reader is not <code>null</code>.
     * @throws IOException
     */
    abstract void doMaterializeValue(IonBinary.Reader reader)
        throws IOException;


    public void deepMaterialize()
    {
        try
        {
            materialize();
        }
        catch (IOException e)
        {
            throw new IonException(e);
        }
    }


    protected void detachFromBuffer()
        throws IOException
    {
        materialize();
        _buffer = null;
    }

    /**
     * Removes this value from its container, ensuring that all data stays
     * available.
     */
    protected final void detachFromContainer() throws IOException
    {
        // TODO this should really copy the buffer to avoid materialization.

        detachFromBuffer();

        _container = null;
        _fieldName = null;
        _fieldSid  = 0;
        setDirty();
    }

    void setFieldName(String name) {
        assert this._fieldName == null;

        // First materialize, because we're gonna mark ourselves dirty.
        makeReady();
        this._fieldName = name;
        this._fieldSid  = 0;
        this.setDirty();
    }

    public String toString() {
        Printer p = new Printer();
        StringBuilder builder = new StringBuilder();
        try {
            p.print(this, builder);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
        return builder.toString();
    }

    protected int getNakedValueLength() throws IOException
    {
        // container overrides this method.
        assert !(this instanceof IonContainer);

        int len;
        if (this._isDirty) {
            assert _hasNativeValue == true || _isPositionLoaded == false;

            if (isNullValue()) return 0;

            len = getNativeValueLength();
        }
        else {
            assert _isPositionLoaded;
            len = pos_getValueOnlyLength();
        }
        return len;
    }

    // TODO document!  What's the difference btw this and getNAKEDvalueLength?
    abstract protected int getNativeValueLength();


    /**
     *
     * @return zero if there's no annotation on this value, else the length
     * of the annotation header:
     *      typedesc + overall value length + symbol list length + symbol ids.
     */
    public int getAnnotationOverheadLength(int valueLen) {
        int len = 0;
        String[] annotationStrings = this.getTypeAnnotations();

        if (annotationStrings != null)
        {
            len = getAnnotationLength();

            // finally add the option length of the overall value, which include
            // the length passed in of the actual data in this value and the length
            // of the annotation list we just added up
            len += IonBinary.lenLenFieldWithOptionalNibble(valueLen + len);

            // this add 1 for the typedesc itself
            len += IonBinary._ib_TOKEN_LEN;
        }

        return len;
    }

    int getAnnotationLength() {
        int len = 0;
        assert (this._annotations != null);

        LocalSymbolTable symtab =  this.getSymbolTable();
        if (symtab == null) {
            // TODO:  what should we do here?  Perhaps create a default table?
            throw new IonException("can't serialize symbols without a symbol table");
        }

        // first add up the length of the annotations symbol id lengths
        for (int ii=0; ii<_annotations.length; ii++) {
            int symId = this.getSymbolTable().findSymbol(_annotations[ii]);
            if (symId <= 0) {
                throw new IllegalStateException("the annotation must be in the symbol table");
            }
            int vlen = IonBinary.lenVarUInt7(symId);
            len += vlen;
        }
        // then add the length of the symbol list which will preceed the symbols
        len += IonBinary.lenVarUInt7(len);

        return len;
    }

    /**
     * Gets the size of the encoded field name (a single VarUInt7).
     * @return the size, or zero if there's no field name.
     */
    public int getFieldNameOverheadLength() {
        int len = 0;

        if (this._fieldSid == 0 && this._fieldName != null)
        {
            // We haven't interned the symbol, do so now.
            this._fieldSid = this.getSymbolTable().findSymbol(this._fieldName);
            if (this._fieldSid <= 0) {
                throw new IonException("the field name must be in the symbol table, if you're using a name table");
            }
            // we will be using the symbol id in just bit (down below)
        }

        if (this._fieldSid > 0) {
            len = IonBinary.lenVarUInt7(this._fieldSid);
            assert len > 0;
        }

        return len;
    }

    /**
     * Length of the core header.
     * @param valuelen length of the core value.
     * @return at least one.
     */
    public int getTypeDescriptorAndLengthOverhead(int valuelen) {
        int len = IonConstants.BB_TOKEN_LEN;

        if (isNullValue()) return len;

        switch (this.pos_getType()) {
        case IonConstants.tidNull: // null(0)
        case IonConstants.tidBoolean: // boolean(1)
            break;
        case IonConstants.tidPosInt: // 2
        case IonConstants.tidNegInt: // 3
        case IonConstants.tidFloat: // float(4)
        case IonConstants.tidDecimal: // decimal(5)
        case IonConstants.tidTimestamp: // timestamp(6)
        case IonConstants.tidSymbol: // symbol(7)
        case IonConstants.tidString: // string (8)
        case IonConstants.tidClob:   // 9
        case IonConstants.tidBlob:   // 10
        case IonConstants.tidList:   // 11
        case IonConstants.tidSexp:   // 12
            len += IonBinary.lenLenFieldWithOptionalNibble(valuelen);
            break;
        case IonConstants.tidStruct: // 13
            // and we need to force the length to be at least 1 byte (since we
            // have to write the 0 length out even if it is 0
            len += valuelen == 0 ? 1 : IonBinary.lenVarUInt7(valuelen);
            break;
        default:
            throw new IonException("this value has an illegal type descriptor id");
        }
        return len;
    }

    /**
     * Includes: header, field name, annotations, value.
     * @return > 0
     * @throws IOException
     */
    int getFullEncodedSize() throws IOException
    {

        int len = 0; // getTotalValueLength();

        if (this.isNullValue()) {
            // the raw non-annotated, non-fieldnamed, length
            // is 1 for a null container
            len = IonConstants.BB_TOKEN_LEN;
        }
        else {
            // for non-null we start with the length of the
            // contents
            len  = this.getNakedValueLength();

            // we need the value length to know whether the len
            // will overflow out of the low nibble
            len += this.getTypeDescriptorAndLengthOverhead(len);
        }
        // we add the length of the annotations (and the ensuing
        // overhead) and the field name length to the total
        // even when it's null

        // we need to know the aggregate length of the value
        // WITH its typedesc and optional length to know
        // how long the length will be for the the annotation
        // typedesc (since it wraps the values td and len and value)
        len += this.getAnnotationOverheadLength(len);

        // and the field precedes them all and doesn't get caught
        // up in all that "length" business.
        len += this.getFieldNameOverheadLength();

        assert len > 0;
        return len;
    }

    /**
     * @return the number of bytes by which this element's total length has
     * changed.
     * @throws IOException
     */
    public final int updateToken() throws IOException {
        if (!this._isDirty) return 0;

        int old_next_start = _next_start;

        //      | fid | an | tlen | alen | ann | td | vlen | value |
        //      |     |                        |                   |
        //       ^ entry_start                  ^ value_start
        //             ^ + len(fid)                      next_start ^

        int fieldSidLen = 0;
        int newFieldSid = 0;
        if (_fieldName != null) {
            // FIXME if this is an embedded value, field name is a layer up
            assert this._container != null;
            assert this._container.pos_getType() == IonConstants.tidStruct;

            newFieldSid = getSymbolTable().addSymbol(_fieldName);
            fieldSidLen  = IonBinary.lenVarUInt7(newFieldSid);
        }

        int valuelen      = this.getNakedValueLength();
        int tdwithvlenlen = this.getTypeDescriptorAndLengthOverhead(valuelen);

        //_entry_start - doesn't change

        // adjust the _value_start to include the fieldsid and annotations
        _value_td_start = _entry_start + fieldSidLen;
        if (_annotations != null) {
            // the annotation overhead needs to know the total value length
            // as it effects how long the annotation type length is
            int annotationlen = this.getAnnotationOverheadLength(valuelen + tdwithvlenlen);
            _value_td_start += annotationlen;
        }

        _value_content_start = _value_td_start + tdwithvlenlen;

        // the next start include the value and it's header as well
        _next_start = _value_td_start + tdwithvlenlen + valuelen;

        int hn = this.pos_getType();
        int ln = this.computeLowNibble(valuelen);
        _type_desc = IonConstants.makeTypeDescriptorByte(hn, ln);

        // overwrite the sid as everything is computed on the new value
        _fieldSid = newFieldSid;

        // and the delta is how far the end moved
        return _next_start - old_next_start;
    }

    /**
     * Precondition:  _isDirty && _hasNativeValue
     *
     * @param valuelen
     * @return the current type descriptor low nibble for this value.
     *
     * @throws IOException if there's a problem reading binary data while
     * making this computation.
     */
    abstract protected int computeLowNibble(int valuelen) throws IOException;


    protected void shiftTokenAndChildren(int delta)
    {
        assert (!this.isDirty());

        pos_moveAll(delta);
    }

    public void updateSymbolTable(LocalSymbolTable symtab) {
        if (this._annotations != null) {
            for (String s : this._annotations) {
                symtab.addSymbol(s);
            }
        }
    }

    /**
     * Brings the binary buffer in sync with the materialized view.
     * <p>
     * The cumulativePositionDelta counts how many bytes have been
     * inserted/removed from the byte buffer "to the left" of this value.
     * If this value has data in the buffer, its current position must be
     * offset by this amount to find where the data is upon entry to this
     * method.  This method may do further inserts and/or deletes, and it must
     * return the accumulated delta.  For example, if this method (and/or any
     * methods it calls) causes N bytes to be inserted into the buffer, it
     * must return <code>(cumulativePositionDelta + N)</code>.  If M bytes are
     * removed, it must return <code>(cumulativePositionDelta - M)</code>.
     * <p>
     * Note that "inserted into the buffer" is different from "written into the
     * buffer".  Writes don't affect the delta, but calls like
     * {@link Writer#insert(int)} and {@link Writer#remove(int)} do.
     * <p>
     * Preconditions: If we've been encoded before, the existing data is at a
     * position after newPosition.  If we need more space we must insert it.
     * If we're dirty, our token has not been updated (so it still points to
     * the old data.
     * <p>
     * Postconditions: Our token is fully up to date.  isDirty() == false
     *
     * @return the updated cumulativePositionDelta.
     * @throws IOException
     */
    protected final int updateBuffer2(IonBinary.Writer writer, int newPosition,
                                      int cumulativePositionDelta)
        throws IOException
    {
        assert writer != null;
        assert writer.position() == newPosition;

        if (!this._isDirty)
        {
            // We don't need to re-encode, but we may need to move our data
            // and we must update our positions to deal with buffer movement.
            int outdatedPosition = pos_getOffsetAtFieldId();
            int realPosition = outdatedPosition + cumulativePositionDelta;

            int gapSize = realPosition - newPosition;
            assert 0 <= gapSize;
            if (0 < gapSize)
            {
                // Remove the gap to our left
                writer.setPosition(newPosition);  // TODO shouldnt be needed
                writer.remove(gapSize);
                cumulativePositionDelta -= gapSize;
            }

            // Update the position in our token.
            // TODO short-circuit on zero delta.
            shiftTokenAndChildren(cumulativePositionDelta);
            writer.setPosition(this.pos_getOffsetofNextValue());
        }
        else if (_entry_start == -1)
        {
            cumulativePositionDelta =
                updateNewValue(writer, newPosition, cumulativePositionDelta);
            setClean();
        }
        else  // We already have data but it's dirty.
        {
            cumulativePositionDelta =
                updateOldValue(writer, newPosition, cumulativePositionDelta);
            setClean();
        }

        return cumulativePositionDelta;
    }

    protected int updateNewValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert writer.position() == newPosition;

        updateToken();

        assert pos_getOffsetAtFieldId() < 0;

        // since the start is at -1 for a new value, the next value
        // offset will be 1 shy of the actual total length (including
        // the field sid, if that's present)
        int size = this.pos_getOffsetofNextValue() + 1;

        // to change the -1 initial position to become newPosition
        this.pos_setEntryStart(newPosition);

        assert newPosition == pos_getOffsetAtFieldId();

        // Create space for me myself and I.
        writer.insert(size);
        cumulativePositionDelta += size;

        cumulativePositionDelta =
            writeElement(writer, cumulativePositionDelta);

        return cumulativePositionDelta;
    }

    protected int updateOldValue(IonBinary.Writer writer, int newPosition,
                                 int cumulativePositionDelta)
        throws IOException
    {
        assert !(this instanceof IonContainer);
        assert writer.position() == newPosition;

        int oldEndOfValue = pos_getOffsetofNextValue();

        pos_moveAll(cumulativePositionDelta);

        assert newPosition <= pos_getOffsetAtFieldId();

        // Do we need to insert more space?
        assert oldEndOfValue - newPosition > 1;
        // TODO why 1?? shouldn't this be > 0

        updateToken();

        int newEndOfValue = pos_getOffsetofNextValue();
        if (newEndOfValue > oldEndOfValue) {
            // We'll be writing beyond the previous limit, so make more space.
            writer.insert(newEndOfValue - oldEndOfValue);
            cumulativePositionDelta += newEndOfValue - oldEndOfValue;
        }

        int cumulativePositionDelta2 =
            writeElement(writer, cumulativePositionDelta);

        assert cumulativePositionDelta2 == cumulativePositionDelta;

        return cumulativePositionDelta;
    }


    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException
     */
    final int writeElement(IonBinary.Writer writer, int cumulativePositionDelta) throws IOException
    {
        writeFieldName(writer);

        assert writer.position() == this.pos_getOffsetInitialTD();
        writeAnnotations(writer);

        assert writer.position() == this.pos_getOffsetAtValueTD();
        cumulativePositionDelta = writeValue(writer, cumulativePositionDelta);
        assert writer.position() == this.pos_getOffsetofNextValue();

        return cumulativePositionDelta;
    }

    /**
     * Called from {@link #updateBuffer()}.
     * <p>
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException

     */
    void writeFieldName(IonBinary.Writer writer) throws IOException
    {
        if (_container instanceof IonStruct)
        {
            int fieldSid = this.getFieldNameId();
            assert fieldSid > 0;
            assert writer.position() == this.pos_getOffsetAtFieldId();

            writer.writeVarUInt7Value(fieldSid, true);
        }
    }

    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     * @throws IOException
     */
    final void writeAnnotations(IonBinary.Writer writer) throws IOException
    {
        if (this._annotations != null) {
            int valuelen      = this.getNakedValueLength();
            int tdwithvlenlen = this.getTypeDescriptorAndLengthOverhead(valuelen);
            int annotationlen = getAnnotationLength();
            int wrappedLength = valuelen + tdwithvlenlen + annotationlen;

            writer.writeCommonTypeDescWithLen(IonConstants.tidTypedecl,
                                                wrappedLength);
            writer.writeAnnotations(_annotations, this.getSymbolTable());
        }
    }

    protected abstract void doWriteNakedValue(IonBinary.Writer writer,
                                              int valueLen)
        throws IOException;

    /**
     * Precondition: the token is up to date, the buffer is positioned properly,
     * and enough space is available.
     *
     * @return the cumulative position delta at the end of this value.
     * @throws IOException
     */
    protected int writeValue(IonBinary.Writer writer,
                             int cumulativePositionDelta)
        throws IOException
    {
        // overridden by container.
        assert !(this instanceof IonContainer);

        // everyone gets a type descriptor byte (which was set
        // properly during updateToken which needs to have been
        // called before this routine!

        writer.write(this.pos_getTypeDescriptorByte());

        // now we write any data bytes - unless it's null
        int vlen = this.getNativeValueLength();
        if (vlen > 0) {
            switch (this.pos_getLowNibble()) {
            case IonConstants.lnIsNullAtom:
            case IonConstants.lnNumericZero:
                // we don't need to do anything here
                break;
            case IonConstants.lnIsVarLen:
                writer.writeVarUInt7Value(vlen, true);
            default:
                this.doWriteNakedValue(writer, vlen);
                break;

            }
        }
        return cumulativePositionDelta;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  the container class = for structs and sequences (list and sexp)
    //
    //
    abstract public static class container
        extends IonValueImpl implements IonContainer
    {
        /**
         * Only meaningful if {@link #_hasNativeValue}.
         */
        protected ArrayList<IonValue> _contents;

        protected container(int typeDesc) {
            super(typeDesc);
        }


        public int size()
            throws NullValueException
        {
            validateThisNotNull();
            makeReady();
            return _contents.size();
        }


        @Override
        protected int getNakedValueLength() throws IOException
        {
            int length = 0;

            if (this.isDirty()) {
                assert _hasNativeValue == true || _isPositionLoaded == false;
                if (_contents != null) {
                    for (IonValue child : _contents) {
                        IonValueImpl aChild = (IonValueImpl)child;
                        length += aChild.getFullEncodedSize();
                    }
                }
            }
            else {
                int start = this.pos_getOffsetAtActualValue();
                int end = this.pos_getOffsetofNextValue();
                length = end - start;
            }

            return length;
        }

        public boolean isEmpty()
        {
            validateThisNotNull();

            return (size() == 0);
        }

        public void clear()
        {
            if (isNullValue())
            {
                _contents = new ArrayList<IonValue>();
                _hasNativeValue = true;
                setDirty();
            }
            /* TODO: if this is a big container that's not materialized,
             * isEmpty() will do a lot of work materializing it just to throw
             * it out.  Optimization needed.
             */
            else if (!isEmpty())
            {
                _contents.clear();
                setDirty();
            }
        }

        public void makeNull()
        {
            if (!isNullValue())
            {
                _contents = null;
                _hasNativeValue = true;
                setDirty();
            }
        }

        void move_start_helper(int offset) {
            for (IonValue v : _contents) {
                ((IonValueImpl)v).pos_moveAll(offset);
            }
            this.pos_moveAll(offset);
        }

        /**
         * Load all children from binary into our native list.
         * <p/>
         * Postcondition: <code>this._hasNativeValue == true </code>
         * @throws IOException
         */
        @Override
        protected void materialize() throws IOException
        {
            // TODO throw IonException not IOException

            if (! _hasNativeValue)
            {
                // First materialization must be from clean state.
                assert !isDirty() || _buffer == null;
                assert _contents == null;

                if (_buffer != null) {
                    assert _isPositionLoaded == true;

                    IonBinary.Reader reader = this._buffer.reader();
                    reader.sync();
                    materializeAnnotations(reader);

                    if (!isNullValue())
                    {
                        _contents = new ArrayList<IonValue>();
                        // this skips past then td and value len
                        reader.setPosition(this.pos_getOffsetAtActualValue());
                        doMaterializeValue(reader);
                    }
                }
                else {
                    assert _isPositionLoaded == false;
                }

                _hasNativeValue = true;
            }
        }

        /**
         * Overridden by DatagramImpl to handle symtabs.
         * And Overridded by Struct to handle field sids.
         * @throws IOException
         */
        @Override
        protected void doMaterializeValue(Reader reader) throws IOException
        {
            assert reader.position() == this.pos_getOffsetAtActualValue();
            assert this.pos_getType() != IonConstants.tidStruct;

            IonBinary.BufferManager buffer = this._buffer;
            LocalSymbolTable symtab = this.getSymbolTable();
            int end = this.pos_getOffsetofNextValue();
            int pos = reader.position();

            while (pos < end) {
                IonValueImpl child;
                reader.setPosition(pos);
                child = makeValueFromReader(0, reader, buffer, symtab, this);
                _contents.add(child);
                pos = child.pos_getOffsetofNextValue();
            }
        }


        public void deepMaterialize()
        {
            try
            {
                materialize();
            }
            catch (IOException e)
            {
                throw new IonException(e);
            }

            if (_contents != null)
            {
                for (IonValue contained : _contents)
                {
                    contained.deepMaterialize();
                }
            }
        }

        @Override
        protected void detachFromBuffer()
            throws IOException
        {
            materialize();
            if (_contents != null)
            {
                for (IonValue contained : _contents)
                {
                    ((IonValueImpl)contained).detachFromBuffer();
                }
            }
            _buffer = null;
        }

        @Override
        protected void shiftTokenAndChildren(int delta)
        {
            assert (!this.isDirty());

            this.pos_moveAll(delta);

            if (_contents != null)
            {
                //  Move our children's tokens.
                for (IonValue child : _contents)
                {
                    IonValueImpl aChild = (IonValueImpl) child;
                    aChild.shiftTokenAndChildren(delta);
                }
            }
        }


        @Override
        protected int updateNewValue(IonBinary.Writer writer, int newPosition, int cumulativePositionDelta) throws IOException
        {
            assert writer.position() == newPosition;

            updateToken();

            assert pos_getOffsetAtFieldId() < 0;

            // int newValueStart = newPosition + getFieldNameOverheadLength();
            this.pos_setEntryStart(newPosition);

            assert newPosition == pos_getOffsetAtFieldId();

            // Create space for our header; children will make room for
            // themselves. header includes annoations, td, and length
            int headerSize = this.pos_getOffsetAtActualValue() - newPosition;

            writer.insert(headerSize);
            cumulativePositionDelta += headerSize;

            cumulativePositionDelta =
                writeElement(writer, cumulativePositionDelta);

            return cumulativePositionDelta;
        }

        @Override
        protected int updateOldValue(IonBinary.Writer writer, int newPosition,
                                     int cumulativePositionDelta)
            throws IOException
        {
            assert writer.position() == newPosition;

            this.pos_moveAll(cumulativePositionDelta);

            int currentPositionOfContent = pos_getOffsetAtActualValue();
            int currentPositionOfFieldId = pos_getOffsetAtFieldId();

            // The old data is at or to the right of the current position.
            assert newPosition <= currentPositionOfFieldId;

            // Recompute our final offsets and lengths.
            updateToken();

            int newPositionOfContent = pos_getOffsetAtActualValue();
            int headerOverlap = newPositionOfContent - currentPositionOfContent;

            if (headerOverlap > 0) {
                // We need to make more space for the field name & annotations
                // so we don't overwrite the core value.
                writer.insert(headerOverlap);
                cumulativePositionDelta += headerOverlap;
            }

            cumulativePositionDelta =
                writeElement(writer, cumulativePositionDelta);

            return cumulativePositionDelta;
        }


        protected int doWriteContainerContents(IonBinary.Writer writer,
                                               int cumulativePositionDelta)
            throws IOException
        {
            if (_contents != null)
            {
                for (int ii = 0; ii < _contents.size(); ii++)
                {
                    IonValueImpl child = (IonValueImpl) _contents.get(ii);

                    cumulativePositionDelta =
                        child.updateBuffer2(writer, writer.position(),
                                            cumulativePositionDelta);
                }
            }
            return cumulativePositionDelta;
        }


        @Override
        protected void doWriteNakedValue(Writer writer, int valueLen)
            throws IOException
        {
            throw new IonException("unsupported operation");
        }


        @Override
        protected int getNativeValueLength()
        {
            int len = this.pos_getOffsetofNextValue()
                    - this.pos_getOffsetAtActualValue();
            return len;
        }

        /**
         * TODO clarify behavior on null.
         */
        protected IonValueImpl getFirstChild(LocalSymbolTable symtab)
        {
            assert !isDirty();

            makeReady();

            assert this._isMaterialized == true || this._hasNativeValue == true;

            if (this.isNullValue()
             || this._contents.size() < 1
            ) {
                return null;
            }

            IonValueImpl first = (IonValueImpl)this._contents.get(0);
            return first;
        }

        public IonValue get(int index)
            throws NullValueException
        {
            if (isNullValue()) {
                throw new NullValueException();
            }

            makeReady();

            return _contents.get(index);
        }

        /**
         * Materialize this container, append a child, and mark this as dirty.
         * <p>
         * This is protected because it's not a valid call for structs.
         * @param element must not be null.
         * @throws NullPointerException if the element is <code>null</code>.
         */
        protected void add(IonValue element)
            throws NullPointerException
        {
            makeReady();
            int size = (_contents == null ? 0 : _contents.size());

            add(size, element, true);
        }

        protected void add(int index, IonValue element)
            throws ContainedValueException, NullPointerException
        {
            add(index, element, true);
        }

        /**
         * Materialize this container, append a child, and (perhaps) mark this
         * as dirty.
         * <p>
         * TO DO: do we really need setDirty? yes.
         * @param element must not be null.
         * @throws NullPointerException if the element is <code>null</code>.
         */
        protected void add(int index, IonValue element, boolean setDirty)
            throws ContainedValueException, NullPointerException
        {
            // This also verifies that element != null to satisfy contract.

            // FIXME should this recognize system container?
            if (element.getContainer() != null) {
                throw new ContainedValueException();
            }

            final IonValueImpl concrete = ((IonValueImpl)element);

            // TODO: true to reuse the byte array if it is present
            //       and the symbol tables are compatible or
            //       the value is big enough to justify embedding
            //       a copy of its symbol table in the stream
            //       otherwise clear the buffer and re-init the positions
            byte[] bytes = null;
            if (concrete._buffer != null
            && !concrete.isDirty()
            &&
              (
                 concrete.getSymbolTable().isCompatible(this.getSymbolTable())
              || concrete.deservesEmbeddingWithLocalSymbolTable()
              )
             ) {
                // TODO: resuse the bytes that are ready to go
                if (bytes == null) {
                    // just a trick to convince Eclipse to ignore two warning
                    // errors that will persist until this code is filled in
                    throw new IonException("feature not implemented - this code should not be reachable.");
                }
            }
            else {
                // TODO: should we copy the symbols to the parent, if there are any?
                concrete.clear_position_and_buffer();
            }

            makeReady();

            if (_contents == null) {
                _contents = new ArrayList<IonValue>();
                _hasNativeValue = true;
            }
            _contents.add(index, element);

            // We shouldn't force the child to be dirty, since we haven't
            // unsynched its materialized and binary copies.

            concrete._container = this;

            if (setDirty) {
                this.setDirty();
            }
        }


        void clear_position_and_buffer()
        {
            makeReady();

            if (this._contents != null) {
                for (IonValue v : this._contents) {
                    ((IonValueImpl)v).clear_position_and_buffer();
                }
            }
            super.clear_position_and_buffer();
        }

        public boolean remove(IonValue element)
        {
            validateThisNotNull();
            if (element.getContainer() != this) return false;

            // We must already be materialized, else we wouldn't have a child.
            assert _hasNativeValue;

            // Get all the data into the DOM, since the element will be losing
            // its backing store.
            IonValueImpl concrete = (IonValueImpl) element;

            try {
                for (Iterator i = _contents.iterator(); i.hasNext();)
                {
                    IonValue child = (IonValue) i.next();
                    if (child == concrete) // Yes, instance identity.
                    {
                        i.remove();

                        concrete.detachFromContainer();

                        this.setDirty();

                        return true;
                    }
                }
            }
            catch (IOException e) {
                throw new IonException(e);
            }
            String message =
                "element is not in materialized contents of its container";
            throw new IllegalStateException(message);
        }

        public Iterator<IonValue> iterator() {
            validateThisNotNull();
            makeReady();
            return new IonContainerIterator(_contents.iterator(), true);
        }

        /** Encapsulates an iterator and implements a custom remove method */
        protected final class IonContainerIterator
            implements Iterator<IonValue>
        {
            private final Iterator<IonValue> it;
            private final boolean allowRemove;
            private IonValue current;

            public IonContainerIterator(Iterator<IonValue> it,
                                        boolean allowRemove)
            {
                this.it = it;
                this.allowRemove = allowRemove;
            }

            public boolean hasNext()
            {
                return it.hasNext();
            }

            public IonValue next()
            {
                current = it.next();
                return current;
            }

            /**
             * Sets the container to dirty after calling
             * {@link Iterator#remove()} on the encapsulated iterator
             */
            public void remove()
            {
                if (!allowRemove) {
                    throw new UnsupportedOperationException();
                }

                it.remove();
                try {
                    ((IonValueImpl)current).detachFromContainer();
                }
                catch (IOException e) {
                    throw new IonException(e);
                }
                finally {
                    setDirty();
                }
            }
        }
    }



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    //  the container class = for lists, lists of both data and expressions
    //
    //
    public static abstract class list
        extends container
        implements IonSequence
    {

        /**
         * Constructs a sequence backed by a binary buffer.
         *
         * @param typeDesc
         */
        protected list(int typeDesc) {
            super(typeDesc);
            assert !_hasNativeValue;
        }

        /**
         * Constructs a sequence value <em>not</em> backed by binary.
         * @param typeDesc
         * @param makeNull
         */
        protected list(int typeDesc, boolean makeNull) {
            this(typeDesc);
            assert _contents == null;
            assert isDirty();

            if (! makeNull) {
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


        @Override  // Increasing visibility
        public void add(IonValue element)
            throws ContainedValueException, NullPointerException
        {
            super.add(element);
        }


        @Override  // Increasing visibility
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


            assert wrapper._isSystemValue;  // so we can unwrap it
            super.add(wrapper);
        }

        @Override
        protected int computeLowNibble(int valuelen)
            throws IOException
        {
            assert _hasNativeValue;

            if (_contents == null)
            {
                return IonConstants.lnIsNullSequence;
            }

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
            if (vlen > 0) {
                if (vlen >= IonConstants.lnIsVarLen) {
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


    public static class tmplist extends list implements IonList
    {

        ArrayList<IonValueImpl> _tmpelements;

        /**
         * Creates an empty list.
         */
        public tmplist() {
            super(IonConstants.makeTypeDescriptorByte(
                             IonConstants.tidList
                            ,IonConstants.lnNumericZero
                            )
            );
           _tmpelements = new ArrayList<IonValueImpl>();
            setClean();
        }

        @Override
        public void add(IonValue v) {
            add(size(), v, false);
        }

        @Override
        public void add(int index, IonValue v) {
            add(index, v, false);
        }

        @Override
        protected void add(int index, IonValue v, boolean setdirty) {
            this._tmpelements.add((IonValueImpl)v);
        }


        @Override
        public IonValueImpl getFirstChild(LocalSymbolTable ignored)  {
            IonValueImpl value = null;
            if (this._tmpelements.size() > 0) {
                value = get(0);
            }
            return value;
        }

        @Override
        public IonValueImpl get(int ordinal) {
            IonValueImpl value = this._tmpelements.get(ordinal);
            return value;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();

            for (IonValue v : this) {
                sb.append(v.toString());
            }

            return sb.toString();
        }

        public void accept(ValueVisitor visitor) throws Exception
        {
            visitor.visit(this);
        }

        public String[] getTypeAnnotations()
        {
            return null;
        }

        public void setTypeAnnotations(String annotations)
        {
            throw new IonException("unsupported operation");
        }

        @Override
        protected int computeLowNibble(int valuelen)
        {
            throw new IonException("unsupported operation");
        }

        @Override
        public void doMaterializeValue(Reader reader)
            throws IOException
        {
            throw new IonException("unsupported operation");
        }

        @Override
        protected void doWriteNakedValue(Writer writer, int valueLen)
            throws IOException
        {
            throw new IonException("unsupported operation");
        }

        @Override
        protected int getNativeValueLength()
        {
            throw new IonException("unsupported operation");
        }
    }
}
