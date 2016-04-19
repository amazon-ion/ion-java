/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.lite;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.zip.CRC32;
import software.amazon.ion.IonLob;

abstract class IonLobLite
    extends IonValueLite
    implements IonLob
{

    private byte[] _lob_value;

    protected IonLobLite(ContainerlessContext context, boolean isNull)
    {
        super(context, isNull);
    }

    IonLobLite(IonLobLite existing, IonContext context) {
        super(existing, context);
        if (null != existing._lob_value) {
            int size = existing._lob_value.length;
            this._lob_value = new byte[size];
            System.arraycopy(existing._lob_value, 0, this._lob_value, 0, size);
        }
    }

    @Override
    public abstract IonLobLite clone();


    /**
     * Calculate LOB hash code as XOR of seed with CRC-32 of the LOB data.
     * This distinguishes BLOBs from CLOBs
     * @param seed Seed value
     * @return hash code
     */
    protected int lobHashCode(int seed, SymbolTableProvider symbolTableProvider)
    {
        int result = seed;

        if (!isNullValue())  {
            CRC32 crc = new CRC32();
            crc.update(getBytes());
            result ^= (int) crc.getValue();
        }

        return hashTypeAnnotations(result, symbolTableProvider);
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

    /**
     * Get the byte array without copying
     */
    protected byte[] getBytesNoCopy()
    {
        return _lob_value;
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
