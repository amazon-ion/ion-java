// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.lite;

import com.amazon.ion.IonLob;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.zip.CRC32;

/**
 *
 */
abstract class IonLobLite
    extends IonValueLite
    implements IonLob
{

    private byte[] _lob_value;

    protected IonLobLite(IonSystemLite system, boolean isNull)
    {
        super(system, isNull);
    }


    @Override
    public abstract IonLobLite clone();


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
    protected final void copyFrom(IonLobLite source)
    {
        copyValueContentFrom(source);
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
    }

    public final InputStream newInputStream()
    {
        if (_isNullValue())
        {
            return null;
        }
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
        validateThisNotNull();
        return _lob_value.length;
    }

}
