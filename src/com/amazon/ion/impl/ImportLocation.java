package com.amazon.ion.impl;

/**
 * A SymbolToken's import location, allowing for symbols with unknown text to be mapped to a particular slot
 * in a shared symbol table.
 * NOTE: this is currently not publicly accessible, but it is an important step toward being able to correctly
 * round-trip symbols with unknown text from shared symbol tables in different symbol table contexts. See
 * https://github.com/amazon-ion/ion-java/issues/126 . Support is added now to avoid risking the appearance of performance
 * degradation if ImportLocation support were added after initial release of the incremental IonReader.
 */
class ImportLocation {

    // The name of the shared symbol table.
    final String name;

    // The index into the shared symbol table.
    final int sid;

    ImportLocation(String name, int sid) {
        this.name = name;
        this.sid = sid;
    }

    public String getName() {
        return name;
    }

    public int getSid() {
        return sid;
    }

    @Override
    public String toString() {
        return String.format("ImportLocation::{name: %s, sid: %d}", name, sid);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ImportLocation)) {
            return false;
        }
        ImportLocation that = (ImportLocation) o;
        return this.getName().equals(that.getName()) && this.getSid() == that.getSid();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result += 31 * getName().hashCode();
        result += 31 * getSid();
        return result;
    }
}
