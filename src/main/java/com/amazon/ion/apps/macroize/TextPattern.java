// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.impl.IonRawWriter_1_1;

/**
 * A string pattern to match in some source data.
 */
interface TextPattern {

    /**
     * @param candidate a string to evaluate against the pattern.
     * @return true if the candidate matches this pattern.
     */
    boolean matches(String candidate);

    /**
     * Writes this pattern from the given match. It is up to the caller to ensure the given string is actually a match.
     * @param match the match from which to write the pattern.
     * @param table the context to use when writing.
     * @param writer the writer to which the pattern will be written.
     * @param isBinary true if the output format is binary.
     */
    void invoke(String match, ManualEncodingContext table, IonRawWriter_1_1 writer, boolean isBinary);
}
