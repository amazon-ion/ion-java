// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonReader;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroMatcher;
import com.amazon.ion.impl.macro.MacroRef;

/**
 * A {@link MacroMatcher} that uses a {@link ManualEncodingContext} and can produce {@link SuggestedSignature}s.
 */
class MacroizeMacroMatcher extends MacroMatcher {

    public MacroizeMacroMatcher(IonReader macroReader, ManualEncodingContext symbolTable) {
        super(macroReader, ref -> symbolTable.getMacro(((MacroRef.ByName) ref).getName()));
        symbolTable.addMacro(name(), macro());
    }

    /**
     * @return the suggested signature for this matcher.
     */
    SuggestedSignature getSignature() {
        SuggestedSignature signature = new SuggestedSignature();
        for (Macro.Parameter parameter : macro().getSignature()) {
            switch (parameter.getCardinality()) {
                case ZeroOrOne:
                    signature.addOptional(parameter.getVariableName());
                    break;
                case ExactlyOne:
                    signature.addRequired(parameter.getVariableName());
                    break;
                case OneOrMore:
                    throw new UnsupportedOperationException("TODO: + not yet supported");
                case ZeroOrMore:
                    throw new UnsupportedOperationException("TODO: * not yet supported");
            }
        }
        return signature;
    }
}
