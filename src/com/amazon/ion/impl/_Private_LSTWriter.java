package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;

@Deprecated
public class _Private_LSTWriter implements _Private_IonWriter {
    private int depth;
    private SymbolTable symbolTable;
    private State state;
    private LinkedList<SSTImportDescriptor> declaredImports;
    private LinkedList<String> declaredSymbols;
    private final IonCatalog catalog;
    private boolean seenImports;
    private boolean seenSymbols;

    /**
     * Enum for managing our state between API calls.
     * <p>
     * LST_STRUCT: we are in an lst struct at depth 1.
     * PRE_IMPORT_LIST: setfield was called with the symbol 'imports' from LST_STRUCT.
     * IMPORT_LIST: either stepin(List) was called from PRE_IMPORT_LIST, or stepOut from IMPORT_DESCRIPTOR.
     * IMPORT_DESCRIPTOR: StepIn(Struct) from IMPORT_LIST was called or a field/value pair was written.
     * IMPORT_MAX_ID: setfield was called with the symbol 'max_id' from IMPORT_DESCRIPTOR.
     * IMPORT_NAME: setfield was called with the symbol 'name' from IMPORT_DESCRIPTOR.
     * IMPORT_VERSION: setfield was called with the symbol 'version' from IMPORT_DESCRIPTOR.
     * PRE_SYMBOL: setfield was called with the symbol 'symbols' from LST_STRUCT.
     * SYMBOL_LIST: stepin(List) was called from PRE_SYMBOL_LIST.
     */
    private enum State {LST_STRUCT, PRE_IMPORT_LIST, IMPORT_LIST, IMPORT_DESCRIPTOR, IMPORT_MAX_ID, IMPORT_NAME, IMPORT_VERSION, PRE_SYMBOL_LIST, SYMBOL_LIST}

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
   public _Private_LSTWriter(List<SymbolTable> imports, List<String> symbols, IonCatalog inCatalog) {
        catalog = inCatalog;
        declaredSymbols = new LinkedList<String>();
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
        if(state == null) {
            if(depth == 0) {
                state = State.LST_STRUCT;
            } else {
                throw new UnsupportedOperationException("$ion_symbol_table:: was not added to a struct.");
            }
        }
        depth++;
        switch(state) {
            case LST_STRUCT:
                return;
            case PRE_IMPORT_LIST:
                //entering the declared Imports list.
                state = State.IMPORT_LIST;
                declaredImports = new LinkedList<SSTImportDescriptor>();
                seenImports = true;
                break;
            case IMPORT_LIST:
                //entering an import struct.
                state = State.IMPORT_DESCRIPTOR;
                declaredImports.add(new SSTImportDescriptor());
                break;
            case PRE_SYMBOL_LIST:
                //entering a symbol list.
                if(containerType != IonType.LIST) throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
                declaredSymbols = new LinkedList<String>();
                state = State.SYMBOL_LIST;
                seenSymbols = true;
                break;
            case IMPORT_MAX_ID:
            case IMPORT_VERSION:
            case IMPORT_NAME:
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
        if(state == State.LST_STRUCT) {
            state = null;
            //we're at depth 1 in an LST declaration.
            if(seenImports) {
                //we've already absorbed an import declaration.
                LinkedList<SymbolTable> tempImports = new LinkedList<SymbolTable>();
                tempImports.add(symbolTable.getSystemSymbolTable());
                SymbolTable tempTable = null;
                for(SSTImportDescriptor desc : declaredImports) {
                    tempTable = catalog.getTable(desc.name, desc.version);
                    if(tempTable == null) {
                        tempTable = new SubstituteSymbolTable(desc.name, desc.version, desc.maxID);
                    } else if(tempTable.getMaxId() != desc.maxID) {
                        tempTable = new SubstituteSymbolTable(tempTable, desc.version, desc.maxID);
                    }
                    tempImports.add(tempTable);
                }
                symbolTable = new LocalSymbolTable(new LocalSymbolTableImports(tempImports), declaredSymbols);
                seenImports = false;//do we need to refresh any other processing flags?
            } else {
                //if we didnt step out of an import declaration were coming from a list of symbols.
                for(String sym : declaredSymbols) {
                    symbolTable.intern(sym);
                }
            }
        } else {//were inside of a value
            switch (state) {
                case IMPORT_DESCRIPTOR:
                    SSTImportDescriptor temp = declaredImports.getLast();
                    if(temp.maxID == 0 || temp.name == null || temp.version == 0){
                        throw new UnsupportedOperationException("Illegal Shared Symbol Table Import declared in local symbol table." + temp.name + "." + temp.version);
                    }
                    state = State.IMPORT_LIST;
                    break;
                case IMPORT_LIST:
                case SYMBOL_LIST:
                    state = State.LST_STRUCT;
                    break;//we need to intern declaredSymbols on exiting the entire lst declaration(might not have seen import context yet), so this is a no op.
                case PRE_SYMBOL_LIST:
                case IMPORT_MAX_ID:
                case IMPORT_VERSION:
                case IMPORT_NAME:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public void setFieldName(String name) {
        if(state == State.LST_STRUCT) {
            if (name.equals("imports")) {
                if(seenImports) throw new UnsupportedOperationException("Two import declarations found.");
                state = State.PRE_IMPORT_LIST;
            } else if (name.equals("symbols")) {
                if(seenSymbols) throw new UnsupportedOperationException("Two symbol list declarations found.");
                state = State.PRE_SYMBOL_LIST;
            } else {
                throw new UnsupportedOperationException();
            }
        } else if(state == State.IMPORT_DESCRIPTOR) {
            if(name.equals("name")) {
                state = State.IMPORT_NAME;
            } else if(name.equals("version")) {
                state = State.IMPORT_VERSION;
            } else if(name.equals("max_id")) {
                state = State.IMPORT_MAX_ID;
            }
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void writeString(String value) {
        if(state == State.LST_STRUCT) throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        switch(state) {
            case SYMBOL_LIST:
                declaredSymbols.add(value);
                break;
            case IMPORT_NAME:
                declaredImports.getLast().name = value;
                state = State.IMPORT_DESCRIPTOR;
                break;
            default:
                throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");

        }
    }

    public void writeSymbol(String content){
        if(state == State.PRE_IMPORT_LIST && content.equals("$ion_symbol_table")) {
            state = null;
        } else {
            throw new UnsupportedOperationException("Open content unsupported via the managed binary writer");
        }
    }
    public void writeSymbolToken(SymbolToken content) throws IOException {
        writeSymbol(content.getText());
    }

    public void writeInt(long value) {
        if(state == State.IMPORT_VERSION) {
            declaredImports.getLast().version = (int) value;
            state = State.IMPORT_DESCRIPTOR;
        } else if(state == State.IMPORT_MAX_ID) {
            declaredImports.getLast().maxID = (int) value;
            state = State.IMPORT_DESCRIPTOR;
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



    public void addTypeAnnotation(String annotation) {
        throw new UnsupportedOperationException();
    }

    public void addTypeAnnotationSymbol(SymbolToken annotation) {
        throw new UnsupportedOperationException();
    }


    public void setFieldNameSymbol(SymbolToken name) {
        setFieldName(name.getText());
    }

    public boolean isInStruct() {
       return state != State.SYMBOL_LIST && state != State.IMPORT_LIST;
    }
    public IonCatalog getCatalog() {
        throw new UnsupportedOperationException();
    }
    public void setTypeAnnotations(String... annotations) {
        throw new UnsupportedOperationException();
    }
    public void setTypeAnnotationSymbols(SymbolToken... annotations) { throw new UnsupportedOperationException(); }
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