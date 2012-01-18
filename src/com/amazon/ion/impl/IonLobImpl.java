// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.impl.IonImplUtils.EMPTY_BYTE_ARRAY;

import com.amazon.ion.IonException;
import com.amazon.ion.IonLob;
import com.amazon.ion.NullValueException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;

/**
 * The abstract parent of all Ion lob types.
 */
public abstract class IonLobImpl
    extends IonValueImpl
    implements IonLob
{

    private byte[] _lob_value;

    protected IonLobImpl(IonSystemImpl system, int typeDesc)
    {
        super(system, typeDesc);
    }


    @Override
    public abstract IonLobImpl clone();


    /**
     * Calculate LOB hash code as XOR of seed with CRC-32 of the LOB data.
     * This distinguishes BLOBs from CLOBs
     * @param seed Seed value
     * @return hash code
     */
    protected int lobHashCode(int seed)
    {
        int hash_code = seed;
        if (!isNullValue())  {
            CRC32 crc = new CRC32();
            crc.update(getBytes());
            hash_code ^= (int) crc.getValue();
        }
        return hash_code;
    }

    /**
     * this copies the contents of the lob from the source to
     * this instance (or the "null-ness" if the source is null).
     * It delegates up to IonValueImpl to copy the annotations
     * and field name as necessary.
     *
     * @param source instance to copy from; must not be null.
     * Will be materialized as a side-effect.
     */
    protected final void copyFrom(IonLobImpl source)
    {
        copyAnnotationsFrom(source); // materializes this and the source
        byte[] new_bytes;
        if (source._isNullValue()) {
            new_bytes = null;
        }
        else {
            new_bytes = source._lob_value;
        }
        setBytes(new_bytes);
    }

    /**
     * @param source may be null to make this an Ion null value.
     */
    protected final void copyBytesFrom(byte[] source, int offset, int length)
    {
        if (source == null)
        {
            _lob_value = null;
            _isNullValue(true);
        }
        else
        {
            // Avoid allocation if we happen to have the right length.
            if (_lob_value == null || _lob_value.length != length) {
                _lob_value = new byte[length];
            }
            System.arraycopy(source, offset, _lob_value, 0, length);
            _isNullValue(false);
        }
        _hasNativeValue(true);
        setDirty();
    }


    public final InputStream newInputStream()
    {
        if (isNullValue()) return null;

        makeReady();
        // TODO this is inefficient.  Should stream directly from binary.
        return new ByteArrayInputStream(_lob_value);
    }

    @Deprecated
    public final byte[] newBytes()
    {
        return getBytes();
    }

    public final byte[] getBytes()
    {
        makeReady();
        byte[] user_copy;
        if (_isNullValue()) {
            user_copy = null;
        }
        else {
            user_copy = _lob_value.clone();
        }
        return user_copy;
    }

    public final void setBytes(byte[] bytes)
    {
        setBytes(bytes, 0, bytes == null ? 0 : bytes.length);
    }

    public final void setBytes(byte[] bytes, int offset, int length)
    {
        checkForLock();
        copyBytesFrom(bytes, offset, length);
    }


    public final int byteSize()
    {
        makeReady();
        if (_lob_value == null) throw new NullValueException();
        return _lob_value.length;
    }


    @Override
    protected final int getNativeValueLength()
    {
        assert _hasNativeValue() == true;
        if (_lob_value == null) return 0;
        return _lob_value.length;
    }


    @Override
    protected final int computeLowNibble(int valuelen)
    {
        assert _hasNativeValue() == true;

        int ln = 0;
        if (_isNullValue()) { // if (_lob_value == null) {
            ln = _Private_IonConstants.lnIsNullAtom;
        }
        else {
            ln = getNativeValueLength();
            if (ln > _Private_IonConstants.lnIsVarLen) {
                ln = _Private_IonConstants.lnIsVarLen;
            }
        }
        return ln;
    }


    @Override
    protected final void doMaterializeValue(IonBinary.Reader reader)
        throws IOException
    {
        assert this._isPositionLoaded() == true && this._buffer != null;

        // a native value trumps a buffered value
        if (_hasNativeValue()) return;

        // the reader will have been positioned for us
        assert reader.position() == this.pos_getOffsetAtValueTD();

        // we need to skip over the td to get to the good stuff
        int td = reader.read();
        assert (byte)(0xff & td) == this.pos_getTypeDescriptorByte();

        int type = this.pos_getType();
        if (type != _Private_IonConstants.tidClob && type != _Private_IonConstants.tidBlob) {
            throw new IonException("invalid type desc encountered for value");
        }

        int ln = this.pos_getLowNibble();
        switch ((0xf & ln)) {
        case _Private_IonConstants.lnIsNullAtom:
            _lob_value = null;
            _isNullValue(true);
            break;
        case 0:
            _lob_value = EMPTY_BYTE_ARRAY;
            _isNullValue(false);
            break;
        case _Private_IonConstants.lnIsVarLen:
            ln = reader.readVarUIntAsInt();
            // fall through to default:
        default:
            _lob_value = new byte[ln];
            IonBinary.readAll(reader, _lob_value, 0, ln);
            _isNullValue(false);
            break;
        }

        _hasNativeValue(true);
    }

    @Override
    protected final void doWriteNakedValue(IonBinary.Writer writer,
                                           int valueLen)
        throws IOException
    {
        assert valueLen == this.getNakedValueLength();
        assert valueLen > 0;

        writer.write(_lob_value, 0, valueLen);

        return;
    }

    //public boolean oldisNullValue()
    //{
    //    if (!_hasNativeValue()) return super.oldisNullValue();
    //    return (_lob_value == null);
    //}
}
