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

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 */
public interface _Private_IonWriter
    extends IonWriter
{
    /** Mostly for testing at this point, but could be useful public API. */
    IonCatalog getCatalog();

    /**
     * Returns true if the field name has been set either through setFieldName or
     * setFieldId.  This is generally more efficient than calling getFieldName or
     * getFieldId and checking the return type as it does not need to resolve the
     * name through a symbol table.  This returns false if the field name has not
     * been set.
     *
     * @return true if a field name has been set false otherwise
     */
    boolean isFieldNameSet();

    /**
     * Returns the current depth of containers the writer is at.  This is
     * 0 if the writer is at top-level.
     * @return int depth of container nesting
     */
    int getDepth();

    /**
     * Write an Ion version marker symbol to the output.  This
     * is the $ion_1_0 value currently (in later versions the
     * number may change).  In text output this appears as the
     * text symbol.  In binary this will be the symbol id if
     * the writer is in a list, sexp or struct.  If the writer
     * is currently at the top level this will write the
     * "magic cookie" value.
     *
     *  Writing a version marker will reset the symbol table
     *  to be the system symbol table.
     */
    void writeIonVersionMarker() throws IOException;

    /** Indicates whether the writer is stream copy optimized through {@link #writeValue(com.amazon.ion.IonReader)}. */
    public boolean isStreamCopyOptimized();
}
