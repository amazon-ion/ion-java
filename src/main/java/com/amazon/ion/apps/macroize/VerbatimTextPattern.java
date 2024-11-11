// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.impl.IonRawWriter_1_1;
import com.amazon.ion.impl.macro.SystemMacro;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Writes a String value as a make_string invocation whose argument is a symbol. This allows recurring text to be
 * added to the symbol table and encoded using an ID while retaining the String type.
 */
class VerbatimTextPattern implements TextPattern {

    // The strings to write using make_string invocations.
    private final Set<String> targets;

    /**
     * @param context the encoding context.
     * @param strings the strings to be written using make_string invocations.
     */
    VerbatimTextPattern(ManualEncodingContext context, List<String> strings) {
        this.targets = new HashSet<>();
        targets.addAll(strings);
        for (String target : strings) {
            context.internSymbol(target);
        }
    }

    @Override
    public boolean matches(String candidate) {
        return targets.contains(candidate);
    }

    @Override
    public void invoke(String match, ManualEncodingContext table, IonRawWriter_1_1 writer, boolean isBinary) {
        writer.stepInEExp(SystemMacro.MakeString);
        writer.writeSymbol(table.internSymbol(match));
        writer.stepOut();
    }
}
