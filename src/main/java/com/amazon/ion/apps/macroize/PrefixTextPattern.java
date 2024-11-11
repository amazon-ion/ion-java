// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.impl.IonRawWriter_1_1;
import com.amazon.ion.impl.macro.SystemMacro;

import java.util.List;

/**
 * Writes a String value as a make_string invocation whose first argument is a symbol and whose second argument
 * is either a symbol or a string. This allows for strings with common prefixes to be written compactly, even if
 * they may have high-cardinality suffixes.
 */
class PrefixTextPattern implements TextPattern { // TODO unify with SubstringTextPattern?
    private final String commonPrefix;

    /**
     * @param context the encoding context.
     * @param commonPrefix the prefix.
     * @param suffixes recurring suffixes, if any. May be empty. If a suffix not present in this list is encountered
     *                 in the data, that suffix will be written as a string instead of a symbol.
     */
    PrefixTextPattern(ManualEncodingContext context, String commonPrefix, List<String> suffixes) {
        this.commonPrefix = commonPrefix;
        context.internSymbol(commonPrefix);
        for (String suffix : suffixes) {
            context.internSymbol(suffix);
        }
    }

    @Override
    public boolean matches(String candidate) {
        return candidate.startsWith(commonPrefix);
    }

    @Override
    public void invoke(String match, ManualEncodingContext table, IonRawWriter_1_1 writer, boolean isBinary) {
        // TODO consider whether these could/should be written using a custom macro that itself calls make_string.
        writer.stepInEExp(SystemMacro.MakeString);
        writer.stepInExpressionGroup(true);
        writer.writeSymbol(table.internSymbol(commonPrefix));
        String suffix = match.replace(commonPrefix, "");
        if (table.hasSymbol(suffix)) {
            writer.writeSymbol(table.internSymbol(suffix));
        } else {
            writer.writeString(suffix);
        }
        writer.stepOut();
        writer.stepOut();
    }
}
