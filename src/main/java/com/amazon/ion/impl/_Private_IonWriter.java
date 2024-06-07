// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
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

    @FunctionalInterface
    interface IntTransformer {

        /**
         * Transforms an int to another int.
         * @param original the int to transform.
         * @return the transformed int.
         */
        int transform(int original);
    }

    /**
     * Returns the provided int unchanged.
     */
    IntTransformer IDENTITY_INT_TRANSFORMER = i -> i;

    /**
     * Transforms Ion 1.0 local symbol IDs to the equivalent Ion 1.1 local symbol ID. Note: system symbols do not
     * follow this path.
     */
    // TODO change the following once the Ion 1.1 symbol table is finalized. Probably:
    //   sid10 -> sid10 - SystemSymbols.ION_1_0_MAX_ID;
    IntTransformer ION_1_0_SID_TO_ION_1_1_SID = IDENTITY_INT_TRANSFORMER;

    /**
     * Works the same as {@link IonWriter#writeValues(IonReader)}, but transforms all symbol IDs that would otherwise
     * be written verbatim using the given transform function. This can be used to do a system-level transcode of
     * Ion 1.0 data to Ion 1.1 while preserving symbol IDs that point to the same text.
     * @param reader the reader from which to transcode.
     * @param symbolIdTransformer the symbol ID transform function.
     * @throws IOException if thrown during write.
     */
    default void writeValues(IonReader reader, IntTransformer symbolIdTransformer) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Works the same as {@link IonWriter#writeValue(IonReader)}, but transforms all symbol IDs that would otherwise
     * be written verbatim using the given transform function. This can be used to do a system-level transcode of
     * Ion 1.0 data to Ion 1.1 while preserving symbol IDs that point to the same text.
     * @param reader the reader from which to transcode.
     * @param symbolIdTransformer the symbol ID transform function.
     * @throws IOException if thrown during write.
     */
    default void writeValue(IonReader reader, IntTransformer symbolIdTransformer) throws IOException {
        throw new UnsupportedOperationException();
    }
}
