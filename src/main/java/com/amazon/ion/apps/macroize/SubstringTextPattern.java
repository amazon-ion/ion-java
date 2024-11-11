// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.impl.IonRawWriter_1_1;
import com.amazon.ion.impl.macro.SystemMacro;

import java.util.List;

/**
 * Writes a String value as a make_string invocation with a prefix, a recurring substring, and a suffix. This allows for
 * strings with common substrings to be written compactly, even if they may have high-cardinality prefixes and/or
 * suffixes.
 */
class SubstringTextPattern implements TextPattern {

    private final String substring;

    /**
     * @param context the encoding context.
     * @param substring the prefix.
     * @param prefixesAndSuffixes recurring prefixes and/or suffixes, if any. May be empty. If a prefix or suffix
     *                            not present in this list is encountered in the data, it will be written as a string
     *                            instead of a symbol.
     */
    SubstringTextPattern(ManualEncodingContext context, String substring,  List<String> prefixesAndSuffixes) {
        this.substring = substring;
        context.internSymbol(substring);
        for (String prefixOrSuffix : prefixesAndSuffixes) {
            context.internSymbol(prefixOrSuffix);
        }
    }

    @Override
    public boolean matches(String candidate) {
        return candidate.contains(substring);
    }

    private void writeComponent(String component, ManualEncodingContext table, IonRawWriter_1_1 writer) {
        if (table.hasSymbol(component)) {
            writer.writeSymbol(table.internSymbol(component));
        } else {
            writer.writeString(component);
        }
    }

    @Override
    public void invoke(String match, ManualEncodingContext table, IonRawWriter_1_1 writer, boolean isBinary) {
        writer.stepInEExp(SystemMacro.MakeString);
        writer.stepInExpressionGroup(true);
        String[] components = match.split(substring);
        if (!components[0].isEmpty()) {
            writeComponent(components[0], table, writer);
        }
        writer.writeSymbol(table.internSymbol(substring));
        if (components.length > 1 && !components[1].isEmpty()) {
            writeComponent(components[1], table, writer);
        }
        writer.stepOut();
        writer.stepOut();
    }
}
