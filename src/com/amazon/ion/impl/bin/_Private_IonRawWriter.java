/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.bin;

import com.amazon.ion.IonWriter;
import java.io.IOException;

/**
 * An {@link IonWriter} with no symbol table management.
 *
 * @deprecated This is a private API subject to change without notice.
 */
@Deprecated
public interface _Private_IonRawWriter extends IonWriter
{
    /**
     * Sets the current field name to the given symbol ID. It is up to the
     * caller to make sure the given symbol ID has a mapping in the current
     * context's symbol table. The pending field name symbol is cleared when the
     * current value is written via stepIn() or one of the write*() methods.
     * @param sid - a symbol ID
     */
    public void setFieldNameSymbol(int sid);

    /**
     * Sets the full list of pending annotations to the given symbol IDs. Any
     * pending annotation symbol IDs are cleared. It is up to the caller to make
     * sure the given symbol IDs have mappings in the current context's symbol
     * table. The contents of the given array are copied into this writer, so
     * the caller does not need to preserve the array. The list of pending
     * annotation symbol IDs is cleared when the current value is written via
     * stepIn() or one of the write*() methods.
     * @param sids - symbol IDs representing the annotation symbols for the
     *               current value
     */
    public void setTypeAnnotationSymbols(int... sids);

    /**
     * Adds the given symbol ID to the list of pending annotation symbol IDs. It
     * is up to the caller to make sure the given symbol ID has a mapping in the
     * current context's symbol table. The list of pending annotation symbol IDs
     * is cleared when the current value is written via stepIn() or one of the
     * write*() methods.
     * @param sid - a symbol ID
     */
    public void addTypeAnnotationSymbol(int sid);

    /**
     * Directly write the given symbol ID to represent a symbol value. It is up
     * to the caller to make sure the given symbol ID has a mapping in the
     * current context's symbol table.
     * @param sid - a symbol ID
     * @throws IOException
     */
    public void writeSymbolToken(int sid) throws IOException;

    /**
     * Writes a portion of the byte array out as an IonString value.  This
     * copies the portion of the byte array that is written.
     *
     * @param data well-formed UTF-8-encoded bytes to be written.
     * May be {@code null} to represent {@code null.string}.
     * @param offset offset of the first byte in value to write
     * @param length number of bytes to write from value
     * @see IonWriter#writeClob(byte[], int, int)
     * @see IonWriter#writeBlob(byte[], int, int)
     */
    public void writeString(byte[] data, int offset, int length)
        throws IOException;

}
