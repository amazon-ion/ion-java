package com.amazon.ion.impl;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.amazon.ion.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;

public class _Private_LSTWriter implements _Private_IonWriter {
    private int depth;
    private SymbolTable symbolTable;
    private State state;
    private LinkedList<SSTImportDescriptor> decImports;
    private LinkedList<String> decSymbols;
    private IonCatalog catalog;
    private boolean seenImports;

    /**
     * Enum for managing our state between API calls.
     * <p>
     * null: we are in an lst struct at depth 1.
     * preImp: setfield was called with the symbol 'imports' from null.
     * impList: either stepin(List) was called from PreImp, or stepOut from impDesc.
     * impDesc: StepIn(Struct) from impList was called or a field/value pair was written.
     * impMID: setfield was called with the symbol 'max_id' from impDesc.
     * impName: setfield was called with the symbol 'name' from impDesc.
     * impVer: setfield was called with the symbol 'version' from impDesc.
     * preSym: setfield was called with the symbol 'symbols' from null.
     * sym: stepin(List) was called from PreSym.
     */
    private enum State {preImp, impList, impDesc, impMID, impName, impVer,  preSym, sym}

    /**
     * Holds import descriptor info between API calls.
     **/
    private static class SSTImportDescriptor {
        String name;
        int version;
        int maxID;
    }

    /**
     * Constructor for the _Private_LSTWriter
     * <p>
     * Absorbs system processing from user level APIs in the IonManagedBinaryWriter.
     *
     * @param  imports ArrayList<SymbolTable> is populated by the builder.
     * @param  symbols List<String>
     * @param  inCatalog IonCatalog
     */
   public _Private_LSTWriter(ArrayList<SymbolTable> imports, List<String> symbols, IonCatalog inCatalog) {
        catalog = inCatalog;
        decSymbols = new LinkedList<String>();
        if(imports.isEmpty() || !imports.get(0).isSystemTable()){
            imports.add(0, _Private_Utils.systemSymtab(1));
        }
        symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(imports), symbols);
        depth = 0;
    }

    public SymbolTable getSymbolTable() {
        return symbolTable;
    }

    public void stepIn(IonType containerType) throws IOException {
        depth++;
        if(state == null) return;
        switch(state) {
            case preImp:
                //entering the decImports list.
                state = State.impList;
                decImports = new LinkedList<SSTImportDescriptor>();
                seenImports = true;
                //we need to delete all context when we step out?
                break;
            case impList:
                //entering an import struct.
                state = State.impDesc;
                decImports.add(new SSTImportDescriptor());
                break;
            case preSym:
                //entering a symbol list.
                if(containerType != IonType.LIST) throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
                decSymbols = new LinkedList<String>();
                state = State.sym;
                break;
            case impMID:
            case impVer:
            case impName:
                throw new UnsupportedOperationException();
            default:
                switch(containerType) {
                    case LIST:
                    case STRUCT:
                    case SEXP:
                        throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
                }
        }
    }

    public void stepOut() {
        depth--;
        if(state == null) {
            //we're at depth 1 in an LST declaration.
            if(seenImports) {
                //we've already absorbed an import declaration.
                SSTImportDescriptor temp = decImports.getLast();
                if(temp.maxID == 0 || temp.name == null || temp.version == 0) throw new UnsupportedOperationException("Illegal Shared Symbol Table Import declared in local symbol table." + temp.name + "." + temp.version);
                LinkedList<SymbolTable> tempImports = new LinkedList<SymbolTable>();
                tempImports.add(symbolTable.getSystemSymbolTable());
                SymbolTable tempTable = null;
                for(SSTImportDescriptor desc : decImports) {
                    tempTable = catalog.getTable(desc.name, desc.version);
                    if(tempTable == null) {
                        tempTable = new SubstituteSymbolTable(desc.name, desc.version, desc.maxID);
                    } else if(tempTable.getMaxId() != desc.maxID) {
                        tempTable = new SubstituteSymbolTable(tempTable, desc.version, desc.maxID);
                    }
                    tempImports.add(tempTable);
                }
                symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(tempImports), decSymbols);
                seenImports = false;//do we need to refresh any other processing flags?
            } else {
                //if we didnt step out of an import declaration were coming from a list of symbols.
                for(String sym : decSymbols) {
                    symbolTable.intern(sym);
                }
            }
        } else {//were inside of a value
            switch (state) {
                case impDesc:
                    state = State.impList;
                    break;
                case impList:
                case sym:
                    state = null;
                    break;//we need to intern decsymbols on exiting the entire lst declaration(might not have seen import context yet), so this is a no op.
                case preSym:
                case impMID:
                case impVer:
                case impName:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public void setFieldName(String name) {
        if(state == null) {
            if (name.equals("imports")) {
                if(seenImports) throw new UnsupportedOperationException("Two import declarations found.");
                state = State.preImp;
            } else if (name.equals("symbols")) {
                if(decSymbols.size() > 0) throw new UnsupportedOperationException("Two symbol list declarations found.");
                state = State.preSym;
            } else {
                throw new UnsupportedOperationException();
            }
        } else if(state == State.impDesc) {
            if(name.equals("name")) {
                state = State.impName;
            } else if(name.equals("version")) {
                state = State.impVer;
            } else if(name.equals("max_id")) {
                state = State.impMID;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void writeString(String value) {
        if(state == null) throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        switch(state) {
            case sym:
                decSymbols.add(value);
                break;
            case impName:
                decImports.getLast().name = value;
                state = State.impDesc;
                break;
            default:
                throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");

        }
    }

    public void writeSymbol(String content){
        if(state == State.preImp && content.equals("$ion_symbol_table")) {
            state = null;
        } else {
            throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        }
    }
    public void writeSymbolToken(SymbolToken content) throws IOException {
        writeSymbol(content.getText());
    }

    public void writeInt(long value) {
        if(state == State.impVer) {
            decImports.getLast().version = (int) value;
            state = State.impDesc;
        } else if(state == State.impMID) {
            decImports.getLast().maxID = (int) value;
            state = State.impDesc;
        } else {
            throw new UnsupportedOperationException("Open content is unsupported via these APIs.");
        }
    }

    public void writeInt(BigInteger value){
        writeInt(value.longValue());
    }

    public int getDepth() {
        return depth;
    }

    public void writeNull() {

    }

    public void writeNull(IonType type) {

    }
    //This isn't really fulfilling the contract, but we're destroying any open content anyway so screw'em.
    public boolean isInStruct() {
        return state == null || state == State.impDesc;
    }

    public void addTypeAnnotation(String annotation) {
        throw new UnsupportedOperationException();
    }

    public void addTypeAnnotationSymbol(SymbolToken annotation) {
        throw new UnsupportedOperationException();
    }


    public void setFieldNameSymbol(SymbolToken name) {
        setFieldName(name.getText());
    }
    //we aren't really a writer
    public IonCatalog getCatalog() {
        throw new UnsupportedOperationException();
    }
    public void setTypeAnnotations(String... annotations) {
        throw new UnsupportedOperationException();
    }
    public void setTypeAnnotationSymbols(SymbolToken... annotations){
        throw new UnsupportedOperationException();
    }
    public void flush() throws IOException { throw new UnsupportedOperationException(); }
    public void finish() throws IOException { throw new UnsupportedOperationException(); }
    public void close() throws IOException { throw new UnsupportedOperationException(); }
    public boolean isFieldNameSet() {
        throw new UnsupportedOperationException();
    }
    public void writeIonVersionMarker(){
        throw new UnsupportedOperationException();
    }
    public boolean isStreamCopyOptimized(){
        throw new UnsupportedOperationException();
    }
    public void writeValue(IonReader reader) throws IOException { throw new UnsupportedOperationException(); }
    public void writeValues(IonReader reader) throws IOException { throw new UnsupportedOperationException(); }
    public void writeBool(boolean value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeFloat(double value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeDecimal(BigDecimal value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeTimestamp(Timestamp value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeValue(IonValue value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeTimestampUTC(Date value) throws IOException { throw new UnsupportedOperationException(); }
    public <T> T asFacet(Class<T> facetType){ throw new UnsupportedOperationException(); }


    public void writeClob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeClob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
    public void writeBlob(byte[] value) throws IOException { throw new UnsupportedOperationException(); }
    public void writeBlob(byte[] value, int start, int len) throws IOException { throw new UnsupportedOperationException(); }
}