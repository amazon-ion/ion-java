// Copyright (c) 2015 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl.bin;

import static com.amazon.ion.IonType.LIST;
import static com.amazon.ion.IonType.STRUCT;
import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION_1_0_MAX_ID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.SystemSymbols.MAX_ID_SID;
import static com.amazon.ion.SystemSymbols.NAME_SID;
import static com.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static com.amazon.ion.SystemSymbols.VERSION_SID;
import static com.amazon.ion.impl.bin.Symbols.symbol;
import static com.amazon.ion.impl.bin.Symbols.systemSymbol;
import static com.amazon.ion.impl.bin.Symbols.systemSymbolTable;
import static com.amazon.ion.impl.bin.Symbols.systemSymbols;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.IonRawBinaryWriter.StreamMode;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Wraps {@link IonRawBinaryWriter} with symbol table management. */
/*package*/ final class IonManagedBinaryWriter extends AbstractIonWriter
{
    /**
     * Provides the import context for the writer.
     * This class is immutable and shareable across instances.
     */
    /*package*/ static final class ImportedSymbolContext
    {
        public final List<SymbolTable>          parents;
        public final Map<String, SymbolToken>   importedSymbols;
        public final int                        localSidStart;

        /*package*/ ImportedSymbolContext(final List<SymbolTable> imports)
        {

            final List<SymbolTable> mutableParents = new ArrayList<SymbolTable>(imports.size());
            final Map<String, SymbolToken> mutableImportedSymbols = new HashMap<String, SymbolToken>();

            // add in system tokens
            for (final SymbolToken token : systemSymbols())
            {
                mutableImportedSymbols.put(token.getText(), token);
            }

            // add in imports
            int maxSid = ION_1_0_MAX_ID + 1;
            for (final SymbolTable st : imports)
            {
                if (!st.isSharedTable())
                {
                    throw new IonException("Imported symbol table is not shared: " + st);
                }
                if (st.isSystemTable())
                {
                    // ignore
                    continue;
                }
                mutableParents.add(st);
                final Iterator<String> iter = st.iterateDeclaredSymbolNames();
                while (iter.hasNext())
                {
                    final String text = iter.next();
                    if (!mutableImportedSymbols.containsKey(text))
                    {
                        mutableImportedSymbols.put(text, symbol(text, maxSid));
                    }
                    maxSid++;
                }
            }

            this.parents = unmodifiableList(mutableParents);
            this.importedSymbols = unmodifiableMap(mutableImportedSymbols);
            this.localSidStart = maxSid;
        }
    }
    /*package*/ static final ImportedSymbolContext ONLY_SYSTEM_IMPORTS =
        new ImportedSymbolContext(Collections.<SymbolTable>emptyList());

    private enum LocalSymbolTableState {
        NONE {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException
            {
                // never generated a table, so emit the IVM
                writer.writeIVM();
            }
        },
        GENERATED_NO_LOCALS {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException
            {
                // never wrote any locals so we only have to pop out one level
                writer.stepOut();
            }
        },
        GENERATED_WITH_LOCALS {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException {
                // close out locals
                writer.stepOut();

                // close out the local symtab struct
                writer.stepOut();
            }
        };

        public abstract void closeTable(IonRawBinaryWriter writer) throws IOException;
    }

    private final ImportedSymbolContext imports;
    private final Map<String, SymbolToken> locals;

    private final IonRawBinaryWriter symbols;
    private final IonRawBinaryWriter user;

    private LocalSymbolTableState localSymbolTableState;

    /*package*/ IonManagedBinaryWriter(final IonManagedBinaryWriterBuilder builder,
                                final OutputStream out)
                                throws IOException
    {
        // TODO expose preallocation mode
        this.symbols = new IonRawBinaryWriter(
            builder.provider,
            builder.symbolsBlockSize,
            out,
            StreamMode.NO_CLOSE,
            builder.preallocationMode
        );
        this.user = new IonRawBinaryWriter(
            builder.provider,
            builder.userBlockSize,
            out,
            StreamMode.CLOSE,
            builder.preallocationMode
        );

        this.imports = builder.imports;
        this.locals = new LinkedHashMap<String, SymbolToken>();
        this.localSymbolTableState = LocalSymbolTableState.NONE;
    }

    // Symbol Table Management

    private void startLocalSymbolTableIfNeeded() throws IOException
    {
        if (localSymbolTableState == LocalSymbolTableState.NONE)
        {
            symbols.writeIVM();
            symbols.addTypeAnnotation(systemSymbol(ION_SYMBOL_TABLE_SID));
            symbols.stepIn(STRUCT);
            {
                if (imports.parents.size() > 0)
                {
                    symbols.setFieldNameSymbol(systemSymbol(IMPORTS_SID));
                    symbols.stepIn(LIST);
                    for (final SymbolTable st : imports.parents)
                    {
                        symbols.stepIn(STRUCT);
                        {
                            symbols.setFieldNameSymbol(systemSymbol(NAME_SID));
                            symbols.writeString(st.getName());
                            symbols.setFieldNameSymbol(systemSymbol(VERSION_SID));
                            symbols.writeInt(st.getVersion());
                            symbols.setFieldNameSymbol(systemSymbol(MAX_ID_SID));
                            symbols.writeInt(st.getMaxId());
                        }
                        symbols.stepOut();
                    }
                    symbols.stepOut();
                }
            }
            // XXX no step out
            localSymbolTableState = LocalSymbolTableState.GENERATED_NO_LOCALS;
        }
    }

    private void startLocalSymbolTableSymbolListIfNeeded() throws IOException
    {
        if (localSymbolTableState == LocalSymbolTableState.GENERATED_NO_LOCALS)
        {
            symbols.setFieldNameSymbol(systemSymbol(SYMBOLS_SID));
            symbols.stepIn(LIST);
            // XXX no step out

            localSymbolTableState = LocalSymbolTableState.GENERATED_WITH_LOCALS;
        }
    }

    private void endLocalSymbolTableIfNeeded() throws IOException
    {
        localSymbolTableState.closeTable(symbols);
        // TODO be more configurable with respect to local symbol table caching
        locals.clear();

        // flush out to the stream.
        symbols.finish();
        localSymbolTableState = LocalSymbolTableState.NONE;
    }

    private SymbolToken intern(final String text)
    {
        try
        {
            SymbolToken token = imports.importedSymbols.get(text);
            if (token != null)
            {
                if (token.getSid() > ION_1_0_MAX_ID)
                {
                    // using a symbol from an import triggers emitting locals
                    startLocalSymbolTableIfNeeded();
                }
                return token;
            }
            // try the locals
            token = locals.get(text);
            if (token == null)
            {
                // if we got here, this is a new symbol and we better start up the locals
                startLocalSymbolTableIfNeeded();
                startLocalSymbolTableSymbolListIfNeeded();

                token = symbol(text, imports.localSidStart + locals.size());
                locals.put(text, token);

                symbols.writeString(text);
            }
            return token;
        }
        catch (final IOException e)
        {
            throw new IonException("Error synthesizing symbols", e);
        }
    }

    private static final SymbolTable[] EMPTY_SYMBOL_TABLE_ARRAY = new SymbolTable[0];

    /**
     * {@inheritDoc}
     * <p>
     * This implementation returns a snapshot of the symbol table context and should be used sparingly.
     * This is done because the underlying symbol state is volatile with respect to the writer and may
     * be confusing if the underlying symbol data changes underneath the returned instance.
     */
    public SymbolTable getSymbolTable()
    {
        if (localSymbolTableState == LocalSymbolTableState.NONE && imports.parents.isEmpty())
        {
            return Symbols.systemSymbolTable();
        }

        final Map<String, SymbolToken> localsCopy = unmodifiableMap(new HashMap<String, SymbolToken>(locals));
        final SymbolTable[] importsArray = imports.parents.toArray(EMPTY_SYMBOL_TABLE_ARRAY);
        return new AbstractSymbolTable(null, 0)
        {
            public Iterator<String> iterateDeclaredSymbolNames()
            {
                return localsCopy.keySet().iterator();
            }

            public int getMaxId()
            {
                return imports.localSidStart + localsCopy.size();
            }

            public SymbolTable[] getImportedTables()
            {
                return importsArray;
            }

            public int getImportedMaxId()
            {
                return imports.localSidStart - 1;
            }

            public boolean isSystemTable() { return false; }
            public boolean isSubstitute()  { return false; }
            public boolean isSharedTable() { return false; }
            public boolean isLocalTable()  { return true; }
            public boolean isReadOnly()    { return true; }

            public SymbolTable getSystemSymbolTable()
            {
                return systemSymbolTable();
            }

            public SymbolToken intern(final String text)
            {
                final SymbolToken token = find(text);
                if (token == null)
                {
                    throw new IonException("Cannot inter into immutable local symbol table");
                }
                return null;
            }

            public String findKnownSymbol(final int id)
            {
                // TODO decide if it is worth making this better than O(N) -- requires more state tracking (but for what use case?)
                for (final SymbolToken token : imports.importedSymbols.values())
                {
                    if (token.getSid() == id)
                    {
                        return token.getText();
                    }
                }
                for (final SymbolToken token : localsCopy.values())
                {
                    if (token.getSid() == id)
                    {
                        return token.getText();
                    }
                }
                return null;
            }

            public SymbolToken find(final String text)
            {
                final SymbolToken token = imports.importedSymbols.get(text);
                if (token != null)
                {
                    return token;
                }
                return localsCopy.get(text);
            }
        };
    }

    // Current Value Meta

    public void setFieldName(final String name)
    {
        final SymbolToken token = intern(name);
        user.setFieldNameSymbol(token);
    }

    public void setFieldNameSymbol(SymbolToken token)
    {
        user.setFieldNameSymbol(token);
    }

    public void setTypeAnnotations(final String... annotations)
    {
        final SymbolToken[] tokens = new SymbolToken[annotations.length];
        for (int i = 0; i < tokens.length; i++)
        {
            tokens[i] = intern(annotations[i]);
        }
        user.setTypeAnnotationSymbols(tokens);
    }

    public void setTypeAnnotationSymbols(final SymbolToken... annotations)
    {
        user.setTypeAnnotationSymbols(annotations);
    }

    public void addTypeAnnotation(final String annotation)
    {
        final SymbolToken token = intern(annotation);
        user.addTypeAnnotation(token);
    }

    // Container Manipulation

    public void stepIn(final IonType containerType) throws IOException
    {
        user.stepIn(containerType);
    }

    public void stepOut() throws IOException
    {
        user.stepOut();
    }

    public boolean isInStruct()
    {
        return user.isInStruct();
    }

    // Write Value Methods

    public void writeNull() throws IOException
    {
        user.writeNull();
    }

    public void writeNull(final IonType type) throws IOException
    {
        user.writeNull(type);
    }

    public void writeBool(final boolean value) throws IOException
    {
        user.writeBool(value);
    }

    public void writeInt(long value) throws IOException
    {
        user.writeInt(value);
    }

    public void writeInt(final BigInteger value) throws IOException
    {
        user.writeInt(value);
    }

    public void writeFloat(final double value) throws IOException
    {
        user.writeFloat(value);
    }

    public void writeDecimal(final BigDecimal value) throws IOException
    {
        user.writeDecimal(value);
    }

    public void writeTimestamp(final Timestamp value) throws IOException
    {
        user.writeTimestamp(value);
    }

    public void writeSymbol(String content) throws IOException
    {
        final SymbolToken token = intern(content);
        user.writeSymbolToken(token);
    }

    public void writeSymbolToken(final SymbolToken token) throws IOException
    {
        user.writeSymbolToken(token);
    }

    public void writeString(final String value) throws IOException
    {
        user.writeString(value);
    }

    public void writeClob(byte[] data) throws IOException
    {
        user.writeClob(data);
    }

    public void writeClob(final byte[] data, final int offset, final int length) throws IOException
    {
        user.writeClob(data, offset, length);
    }

    public void writeBlob(byte[] data) throws IOException
    {
        user.writeBlob(data);
    }

    public void writeBlob(final byte[] data, final int offset, final int length) throws IOException
    {
        user.writeBlob(data, offset, length);
    }

    // Stream Terminators

    public void flush() throws IOException {}

    public void finish() throws IOException
    {
        endLocalSymbolTableIfNeeded();
        user.finish();
    }

    public void close() throws IOException
    {
        finish();
        symbols.close();
        user.close();
    }
}
