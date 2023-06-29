// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.SymbolToken;
import com.amazon.ion.UnknownSymbolException;

/**
 * SymbolToken implementation that includes ImportLocation.
 */
class SymbolTokenWithImportLocation implements _Private_SymbolToken {

    // The symbol's text, or null if the text is unknown.
    private final String text;

    // The local symbol ID of this symbol within a particular local symbol table.
    private final int sid;

    // The import location of the symbol (only relevant if the text is unknown).
    private final ImportLocation importLocation;

    SymbolTokenWithImportLocation(String text, int sid, ImportLocation importLocation) {
        this.text = text;
        this.sid = sid;
        this.importLocation = importLocation;
    }

    @Override
    public String getText() {
        return text;
    }

    @Override
    public String assumeText() {
        if (text == null) {
            throw new UnknownSymbolException(sid);
        }
        return text;
    }

    @Override
    public int getSid() {
        return sid;
    }

    // Will be @Override once added to the SymbolToken interface.
    public ImportLocation getImportLocation() {
        return importLocation;
    }

    @Override
    public String toString() {
        return String.format("SymbolToken::{text: %s, sid: %d, importLocation: %s}", text, sid, importLocation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SymbolToken)) return false;

        // NOTE: once ImportLocation is available via the SymbolToken interface, it should be compared here
        // when text is null.
        SymbolToken other = (SymbolToken) o;
        if (getText() == null || other.getText() == null) {
            return getText() == other.getText();
        }
        return getText().equals(other.getText());
    }

    @Override
    public int hashCode() {
        if (getText() != null) return getText().hashCode();
        return 0;
    }
}
