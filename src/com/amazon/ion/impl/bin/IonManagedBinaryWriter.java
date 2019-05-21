/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.impl.bin;

import static com.amazon.ion.IonType.LIST;
import static com.amazon.ion.IonType.STRUCT;
import static com.amazon.ion.SystemSymbols.*;
import static com.amazon.ion.impl.bin.Symbols.systemSymbol;


import com.amazon.ion.*;
import com.amazon.ion.impl._Private_IonWriter;
import com.amazon.ion.impl._Private_LSTWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import com.amazon.ion.impl.bin.IonRawBinaryWriter.StreamCloseMode;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.StreamFlushMode;



/** Wraps {@link IonRawBinaryWriter} with symbol table management. */
/*package*/ final class IonManagedBinaryWriter extends AbstractIonWriter  implements _Private_IonManagedWriter {
    private final IonCatalog                    catalog;
    private final List<SymbolTable>             fallbackImports;
    private boolean                             closed = false;
    private boolean                             flushed = false;
    private boolean                             hasUnflushedLocalSymbols = false;
    private boolean                             lstRequired = false;
    private _Private_LSTWriter                  lstWriter;
    private IonRawBinaryWriter                  user;
    private IonRawBinaryWriter                  symbols;
    private _Private_IonWriter                  currentWriter;
    private SymbolTable                         lst;
    private int                                 lstIndex;
    private final int                           maxSysId;

    /**
     * This class is surfaced to the user as BinaryWriter/IonWriter.
     * It manages symboltable reconciliation for the user as well as system processing.
     * It only guarantees data model fidelity, system processing of any kind may be absorbed.
     * @param builder a builder supplied by the user to preconfigure writer context.
     * @param out where the payload will be written to.
     * @throws IOException
     */
    /*package*/ IonManagedBinaryWriter(final _Private_IonManagedBinaryWriterBuilder builder,
                                       final OutputStream out)
            throws IOException
    {
        super(builder.optimization);
        //this writer is used to serialize symboltables from LST objects.
        //LST contexts are altered by user input and are thus dynamic during the lifetime of this writer.
        symbols = new IonRawBinaryWriter(
            builder.provider,
            builder.symbolsBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer.
            StreamCloseMode.NO_CLOSE,
            StreamFlushMode.NO_FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );
        user = new IonRawBinaryWriter(
            builder.provider,
            builder.userBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer.
            StreamCloseMode.CLOSE,
            StreamFlushMode.FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );

        //fallback imports are resupplied as the writer context after the user calls finish() and reuses the writer.
        //local symbols are not automatically included in post finish() contexts.
        currentWriter = user;
        catalog = builder.catalog;
        if(builder.imports == null) {
            fallbackImports = new ArrayList<SymbolTable>();
        } else {
            fallbackImports = new ArrayList<SymbolTable>(builder.imports);
        }

        //someone is using a symboltable to populate our current context.
        if (builder.initialSymbolTable != null) {
            lstIndex = builder.initialSymbolTable.getImportedMaxId();
            ArrayList<String> temp = new ArrayList<String>();
            final Iterator<String> symbolIter = builder.initialSymbolTable.iterateDeclaredSymbolNames();
            while (symbolIter.hasNext()) {
                temp.add(symbolIter.next());
            }
            lstWriter = new _Private_LSTWriter(new ArrayList(Arrays.asList(builder.initialSymbolTable.getImportedTables())), temp, catalog);
            lst = lstWriter.getSymbolTable();
            lstIndex = lst.getImportedMaxId();
        } else {
            lstWriter = new _Private_LSTWriter(fallbackImports, Collections.<String>emptyList(), catalog);
            lst = lstWriter.getSymbolTable();
            lstIndex = lst.getImportedMaxId();
        }
        maxSysId = lst.getSystemSymbolTable().getMaxId();
    }

    // Compatibility with Implementation Writer Interface

    public IonCatalog getCatalog()
    {
        return catalog;
    }

    public boolean isFieldNameSet()
    {
        return user.isFieldNameSet();
    }

    public void writeIonVersionMarker() throws IOException {
        finish();
    }

    public int getDepth()
    {
        return currentWriter.getDepth();
    }


    //these should be inverted so that calling intern x text results in a new symboltoken if none currently exist, thus we can support repeated symboltokens
    private SymbolToken intern(final String text) {
        if (text == null) return null;
        SymbolToken token = lst.intern(text);
        hasUnflushedLocalSymbols |= token.getSid() > maxSysId;
        return token;
    }

    private SymbolToken intern(final SymbolToken token) {
        if (token == null) return null;
        String text = token.getText();
        if (text == null){
            int sid = token.getSid();
            if(sid > lst.getMaxId()) throw new UnknownSymbolException(sid);
            hasUnflushedLocalSymbols |= token.getSid() > maxSysId;
            return token;

        }
        return intern(text);
    }

    //a symboltable reference is not guaranteed to reflect the context of the writer.
    //user input can alter it or replace it entirely.
    public SymbolTable getSymbolTable() {
        return lst;
    }

    public void setFieldName(final String name) {
        if (!isInStruct()) throw new IllegalStateException("IonWriter.setFieldName() must be called before writing a value into a struct.");
        if (name == null) throw new NullPointerException("Null field name is not allowed.");
        currentWriter.setFieldNameSymbol(intern(name));
    }

    public void setFieldNameSymbol(SymbolToken token) {
        currentWriter.setFieldNameSymbol(intern(token));
    }

    public void setTypeAnnotations(final String... annotations) {
        if (annotations == null) {
            currentWriter.setTypeAnnotationSymbols();
        } else {
            SymbolToken[] tokens = new SymbolToken[annotations.length];
            for (int i = 0; i < tokens.length; i++) {
                tokens[i] = intern(annotations[i]);
            }
            currentWriter.setTypeAnnotationSymbols(tokens);
        }
    }

    public void setTypeAnnotationSymbols(final SymbolToken... annotations)
    {
        if (annotations == null) {
            currentWriter.setTypeAnnotationSymbols();
        } else {
            for (int i = 0; i < annotations.length; i++) {
                annotations[i] = intern(annotations[i]);//this is going to degrade perf badly TODO fix when import descriptors are added to ST
            }
            currentWriter.setTypeAnnotationSymbols(annotations);
        }
    }

    public void addTypeAnnotation(final String annotation) {
        currentWriter.addTypeAnnotationSymbol(intern(annotation));
    }

    public void addTypeAnnotationSymbol(final SymbolToken annotation) {
        currentWriter.addTypeAnnotationSymbol(intern(annotation));
    }

    // Container Manipulation
    public void stepIn(final IonType containerType) throws IOException
    {
        if(currentWriter.getDepth() == 0 && user.hasTopLevelSymbolTableAnnotation() && containerType == STRUCT){//only true when the user writer wrote the lst st.
            currentWriter = lstWriter;
            user.clearAnnotations();//empties the prepped annotations on the user writer

        }
        currentWriter.stepIn(containerType);
    }

    public void stepOut() throws IOException
    {
        currentWriter.stepOut();
        if(currentWriter == lstWriter && currentWriter.getDepth() == 0) {//depth shouldve been 1.
            currentWriter = user;
            SymbolTable tempLST = lstWriter.getSymbolTable();
            if(lst != tempLST) {//we found imported tables and created a new lst object so the references dont match.
                flush();
                lst = tempLST;
                lstIndex = lst.getImportedMaxId();
            }
        }
    }

    public boolean isInStruct()
    {
        return currentWriter.isInStruct();
    }

    // Write Value Methods
    public void writeNull() throws IOException {
        currentWriter.writeNull();
    }

    public void writeNull(final IonType type) throws IOException {
        currentWriter.writeNull(type);
    }

    public void writeBool(final boolean value) throws IOException {
        currentWriter.writeBool(value);
    }

    public void writeInt(long value) throws IOException {
        currentWriter.writeInt(value);
    }

    public void writeInt(final BigInteger value) throws IOException {
        currentWriter.writeInt(value);
    }

    public void writeFloat(final double value) throws IOException {
        currentWriter.writeFloat(value);
    }

    public void writeDecimal(final BigDecimal value) throws IOException {
        currentWriter.writeDecimal(value);
    }

    public void writeTimestamp(final Timestamp value) throws IOException {
        currentWriter.writeTimestamp(value);
    }

    public void writeSymbol(String content) throws IOException {
        writeSymbolToken(intern(content));
    }

    public void writeSymbolToken(SymbolToken token) throws IOException {
        token = intern(token);
        if (token != null && token.getSid() == ION_1_0_SID && user.getDepth() == 0 && !user.hasAnnotations()) {
            if (user.hasWrittenValuesSinceFinished()) {
                //this explicitly translates SID 2 to an IVM and flushes out local symbol state
                finish();
            }
            return;
        }
        currentWriter.writeSymbolToken(token);
    }

    public void writeString(final String value) throws IOException {
        currentWriter.writeString(value);
    }


    public void writeClob(byte[] data) throws IOException {
        currentWriter.writeClob(data);
    }

    public void writeClob(final byte[] data, final int offset, final int length) throws IOException {
        currentWriter.writeClob(data, offset, length);
    }

    public void writeBlob(byte[] data) throws IOException {
        currentWriter.writeBlob(data);
    }

    public void writeBlob(final byte[] data, final int offset, final int length) throws IOException {
        currentWriter.writeBlob(data, offset, length);
    }

    public void writeBytes(byte[] data, int off, int len) throws IOException {
        user.writeBytes(data, off, len);
    }

    //this will write an original LST or if one has already been written it will write an LST-Append.
    public void flush() throws IOException {
        if (getDepth() == 0) {
            //no op function if the user calls below the top level.
            int maxId = lst.getMaxId();
            if (!flushed && (user.hasWrittenValuesSinceFinished() || isStreamCopyOptimized()))
                symbols.writeIonVersionMarker();
            if (hasUnflushedLocalSymbols || (!flushed && (isStreamCopyOptimized() || lstRequired))) {
                //we need to update our declared symbols to make symbols in the user payload resolveable.
                symbols.addTypeAnnotationSymbol(systemSymbol(ION_SYMBOL_TABLE_SID));
                symbols.stepIn(STRUCT);
                if (flushed) {//append
                    symbols.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
                    symbols.writeSymbolToken(systemSymbol(ION_SYMBOL_TABLE_SID));
                } else {//fresh lst
                    SymbolTable[] tempImports = lst.getImportedTables();
                    if (tempImports.length > 0) {
                        symbols.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
                        symbols.stepIn(LIST);
                        for (final SymbolTable st : tempImports) {
                            symbols.stepIn(STRUCT);
                            symbols.setFieldNameSymbol(systemSymbol(NAME_SID));
                            symbols.writeString(st.getName());
                            symbols.setFieldNameSymbol(systemSymbol(VERSION_SID));
                            symbols.writeInt(st.getVersion());
                            symbols.setFieldNameSymbol(systemSymbol(MAX_ID_SID));
                            symbols.writeInt(st.getMaxId());
                            symbols.stepOut();
                        }
                        symbols.stepOut();
                    }
                }
                if (lstIndex < maxId) {
                    //if our index of declared symbols is less than our context's maxId index then we have new local symbols.
                    symbols.setFieldNameSymbol(systemSymbol(SYMBOLS_SID));
                    symbols.stepIn(LIST);
                    for (int i = this.lstIndex + 1; i <= maxId; i++) {
                        symbols.writeString(this.lst.findKnownSymbol(i));
                    }
                    lstIndex = maxId;
                    symbols.stepOut();
                }
                symbols.stepOut();
                flushed = true;
                hasUnflushedLocalSymbols = false;
            }
            symbols.flush();
            user.flush();
        }
    }

    public void finish() throws IOException {
        //resets the symbol context of the writer and finishes serializing payload.
        //this allows for user to continue writing using the writer from a fresh symbol context as they see fit.
        if (getDepth() != 0) throw new IllegalStateException("IonWriter.finish() can only be called at top-level.");
        flush();
        lstWriter = new _Private_LSTWriter(fallbackImports, new ArrayList<String>(), catalog);
        lst = lstWriter.getSymbolTable();
        lstIndex = lst.getImportedMaxId();
        flushed = false;
    }

    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                flush();
            } catch (IllegalStateException e) {
            } finally {
                try {
                    symbols.close();
                } finally {
                    user.close();
                }
            }
        }
    }

    public _Private_IonRawWriter getRawWriter() {
        return user;
    }

    public void requireLocalSymbolTable() throws IOException {
        lstRequired = true;
    }

}