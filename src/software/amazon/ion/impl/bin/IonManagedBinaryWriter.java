/*
 * Copyright 2015-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl.bin;

import static java.util.Collections.unmodifiableList;
import static software.amazon.ion.IonType.LIST;
import static software.amazon.ion.IonType.STRUCT;
import static software.amazon.ion.SystemSymbols.IMPORTS_SID;
import static software.amazon.ion.SystemSymbols.ION_1_0_MAX_ID;
import static software.amazon.ion.SystemSymbols.ION_1_0_SID;
import static software.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static software.amazon.ion.SystemSymbols.MAX_ID_SID;
import static software.amazon.ion.SystemSymbols.NAME_SID;
import static software.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static software.amazon.ion.SystemSymbols.VERSION_SID;
import static software.amazon.ion.impl.bin.Symbols.symbol;
import static software.amazon.ion.impl.bin.Symbols.systemSymbol;
import static software.amazon.ion.impl.bin.Symbols.systemSymbolTable;
import static software.amazon.ion.impl.bin.Symbols.systemSymbols;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import software.amazon.ion.EmptySymbolException;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonException;
import software.amazon.ion.IonType;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;
import software.amazon.ion.Timestamp;
import software.amazon.ion.impl.PrivateUtils;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamCloseMode;
import software.amazon.ion.impl.bin.IonRawBinaryWriter.StreamFlushMode;

/** Wraps {@link IonRawBinaryWriter} with symbol table management. */
/*package*/ final class IonManagedBinaryWriter extends AbstractIonWriter
{
    private interface SymbolResolver
    {
        /** Resolves a {@link SymbolToken} or returns <code>null</code> if the mapping does not exist. */
        SymbolToken get(String text);
    }

    private interface SymbolResolverBuilder
    {
        /**
         * Adds the given table's mappings to the resolver to be constructed.
         *
         * @param  startSid     The starting local ID.
         * @return the next available ID.
         */
        int addSymbolTable(SymbolTable table, int startSid);

        /** Constructs the resolver from the symbols tables added prior to this call. */
        SymbolResolver build();
    }

    private static final class ImportTablePosition
    {
        public final SymbolTable table;
        public final int startId;

        public ImportTablePosition(final SymbolTable table, final int startId)
        {
            this.table = table;
            this.startId = startId;
        }
    }

    /** Determines how imported symbols are resolved (including system symbols). */
    /*package*/ enum ImportedSymbolResolverMode
    {
        /** Symbols are copied into a flat map, this is useful if the context can be reused across builders. */
        FLAT
        {
            @Override
            /*package*/ SymbolResolverBuilder createBuilder()
            {
                final Map<String, SymbolToken> symbols = new HashMap<String, SymbolToken>();

                // add in system tokens
                for (final SymbolToken token : systemSymbols())
                {
                    symbols.put(token.getText(), token);
                }

                return new SymbolResolverBuilder()
                {
                    public int addSymbolTable(final SymbolTable table, final int startSid)
                    {
                        int maxSid = startSid;
                        final Iterator<String> iter = table.iterateDeclaredSymbolNames();
                        while (iter.hasNext())
                        {
                            final String text = iter.next();
                            if (text != null && !symbols.containsKey(text))
                            {
                                symbols.put(text, symbol(text, maxSid));
                            }
                            maxSid++;
                        }
                        return maxSid;
                    }

                    public SymbolResolver build()
                    {
                        return new SymbolResolver()
                        {
                            public SymbolToken get(final String text)
                            {
                                return symbols.get(text);
                            }
                        };
                    }
                };
            }
        },
        /** Delegates to a set of symbol tables for symbol resolution, this is useful if the context is thrown away frequently. */
        DELEGATE
        {
            @Override
            /*package*/ SymbolResolverBuilder createBuilder()
            {
                final List<ImportTablePosition> imports = new ArrayList<ImportTablePosition>();
                imports.add(new ImportTablePosition(systemSymbolTable(), 1));
                return new SymbolResolverBuilder()
                {
                    public int addSymbolTable(final SymbolTable table, final int startId)
                    {
                        imports.add(new ImportTablePosition(table, startId));
                        return startId + table.getMaxId();
                    }

                    public SymbolResolver build()
                    {
                        return new SymbolResolver()
                        {
                            public SymbolToken get(final String text)
                            {
                                for (final ImportTablePosition tableImport : imports)
                                {
                                    final SymbolToken token = tableImport.table.find(text);
                                    if (token != null)
                                    {
                                        return symbol(text, token.getSid() + tableImport.startId - 1);
                                    }
                                }
                                return null;
                            }
                        };
                    }
                };
            }
        };

        /*package*/ abstract SymbolResolverBuilder createBuilder();
    }

    /**
     * Provides the import context for the writer.
     * This class is immutable and shareable across instances.
     */
    /*package*/ static final class ImportedSymbolContext
    {
        public final List<SymbolTable>          parents;
        public final SymbolResolver             importedSymbols;
        public final int                        localSidStart;

        /*package*/ ImportedSymbolContext(final ImportedSymbolResolverMode mode, final List<SymbolTable> imports)
        {

            final List<SymbolTable> mutableParents = new ArrayList<SymbolTable>(imports.size());

            final SymbolResolverBuilder builder = mode.createBuilder();

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
                maxSid = builder.addSymbolTable(st, maxSid);
            }

            this.parents = unmodifiableList(mutableParents);
            this.importedSymbols = builder.build();
            this.localSidStart = maxSid;
        }
    }
    /*package*/ static final ImportedSymbolContext ONLY_SYSTEM_IMPORTS =
        new ImportedSymbolContext(ImportedSymbolResolverMode.FLAT, Collections.<SymbolTable>emptyList());

    private enum SymbolState {
        SYSTEM_SYMBOLS
        {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException
            {
                // never generated a table, so emit the IVM
                writer.writeIonVersionMarker();
            }
        },
        LOCAL_SYMBOLS_WITH_IMPORTS_ONLY
        {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException
            {
                // never wrote any locals so we only have to pop out one level
                writer.stepOut();
            }
        },
        LOCAL_SYMBOLS
        {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException {
                // close out locals
                writer.stepOut();

                // close out the local symtab struct
                writer.stepOut();
            }
        },
        LOCAL_SYMBOLS_FLUSHED
        {
            @Override
            public void closeTable(final IonRawBinaryWriter writer) throws IOException {
                // we already emitted local symbols -- there is nothing to close
            }
        };

        public abstract void closeTable(IonRawBinaryWriter writer) throws IOException;
    }

    private static class ImportDescriptor
    {
        public String name;
        public int version;
        public int maxId;

        public ImportDescriptor()
        {
            reset();
        }

        public void reset()
        {
            name = null;
            version = -1;
            maxId = -1;
        }

        public boolean isDefined()
        {
            return name != null && version >= 1;
        }

        public boolean isUndefined()
        {
            return name == null && version == -1 && maxId == -1;
        }

        public boolean isMalformed()
        {
            return !isDefined() && !isUndefined();
        }

        @Override
        public String toString()
        {
            return "{name: \"" + name + "\", version: " + version + ", max_id: " + maxId + "}";
        }

    }

    private enum UserState
    {
        /** no-op for all the interceptors. */
        NORMAL
        {
            @Override
            public void beforeStepIn(final IonManagedBinaryWriter self, final IonType type)
            {
                if (self.user.hasTopLevelSymbolTableAnnotation() && type == STRUCT)
                {
                    self.userState = LOCALS_AT_TOP;

                    // record where the user symbol table is written
                    // we're going to clear this out later
                    self.userSymbolTablePosition = self.user.position();
                }
            }

            @Override
            public void afterStepOut(final IonManagedBinaryWriter self) {}

            @Override
            public void writeInt(IonManagedBinaryWriter self, BigInteger value) {}
        },
        LOCALS_AT_TOP
        {
            @Override
            public void beforeStepIn(final IonManagedBinaryWriter self, final IonType type)
            {
                if (self.user.getDepth() == 1)
                {
                    switch (self.user.getFieldId())
                    {
                        case IMPORTS_SID:
                            if (type != LIST)
                            {
                                throw new IllegalArgumentException(
                                    "Cannot step into Local Symbol Table 'symbols' field as non-list: " + type);
                            }
                            self.userState = LOCALS_AT_IMPORTS;
                            break;
                        case SYMBOLS_SID:
                            if (type != LIST)
                            {
                                throw new IllegalArgumentException(
                                    "Cannot step into Local Symbol Table 'symbols' field as non-list: " + type);
                            }
                            self.userState = LOCALS_AT_SYMBOLS;
                            break;
                    }
                }
            }

            @Override
            public void afterStepOut(final IonManagedBinaryWriter self) throws IOException
            {
                if (self.user.getDepth() == 0)
                {
                    // TODO deal with the fact that any open content in the user provided local symbol table is lost...
                    //      the requirements here are not clear through the API contract, we push through
                    //      the logical symbol table content but basically erase open content and erroneous data
                    //      (e.g. integer in the symbol list or some non-struct in the import list)

                    // at this point we have to ditch the user provided symbol table and open our own
                    // since we don't know what's coming after (i.e. new local symbols)
                    self.user.truncate(self.userSymbolTablePosition);

                    // flush out the pre-existing symbol and user content before the user provided symbol table
                    self.finish();

                    // replace the symbol table context with the user provided one
                    // TODO determine if the resolver mode should be configurable for this use case
                    self.imports = new ImportedSymbolContext(ImportedSymbolResolverMode.DELEGATE, self.userImports);

                    // explicitly start the local symbol table with no version marker
                    // in case we need the previous symbols
                    self.startLocalSymbolTableIfNeeded(/*writeIVM*/ false);

                    // let's go intern all of the local symbols that were provided
                    // note that this may erase out redundant locals
                    for (final String text : self.userSymbols)
                    {
                        // go and intern all of the locals now that we have context built
                        self.intern(text);
                    }

                    // clear transient work state
                    self.userSymbolTablePosition = 0L;
                    self.userCurrentImport.reset();
                    self.userImports.clear();
                    self.userSymbols.clear();

                    self.userState = NORMAL;
                }
            }
        },
        LOCALS_AT_IMPORTS
        {
            @Override
            public void beforeStepIn(final IonManagedBinaryWriter self, final IonType type)
            {
                if (type != STRUCT)
                {
                    throw new IllegalArgumentException(
                        "Cannot step into non-struct in Local Symbol Table import list: " + type);
                }
            }

            @Override
            public void afterStepOut(final IonManagedBinaryWriter self)
            {
                switch (self.user.getDepth())
                {
                    // finishing up a import struct
                    case 2:
                        final ImportDescriptor desc = self.userCurrentImport;
                        if (desc.isMalformed())
                        {
                            throw new IllegalArgumentException("Invalid import: " + desc);
                        }
                        if (desc.isDefined())
                        {
                            SymbolTable symbols =
                                self.catalog.getTable(desc.name, desc.version);
                            if (symbols == null)
                            {
                                if (desc.maxId == -1)
                                {
                                    throw new IllegalArgumentException(
                                        "Import is not in catalog and no max ID provided: " + desc);
                                }

                                // TODO determine what the correct behavior here is...
                                // we don't know what the imports are in the context given
                                // this is somewhat problematic, but let's put in a substitute
                                // in case this is intentional
                                symbols = Symbols.unknownSharedSymbolTable(desc.name, desc.version, desc.maxId);
                            }
                            final boolean hasDeclaredMaxId = desc.maxId != -1;
                            final boolean declaredMaxIdMatches = desc.maxId == symbols.getMaxId();
                            final boolean declaredVersionMatches = desc.version == symbols.getVersion();
                            if (hasDeclaredMaxId && (!declaredMaxIdMatches || !declaredVersionMatches))
                            {
                                // the max ID doesn't match, so we need a substitute
                                symbols = PrivateUtils.newSubstituteSymtab(symbols, desc.version, desc.maxId);
                            }
                            self.userImports.add(symbols);
                        }
                        break;
                    // done with the import list
                    case 1:
                        self.userState = LOCALS_AT_TOP;
                        break;
                }
            }

            @Override
            public void writeString(final IonManagedBinaryWriter self, final String value)
            {
                if (self.user.getDepth() == 3 && self.user.getFieldId() == NAME_SID)
                {
                    if (value == null)
                    {
                        throw new NullPointerException("Cannot have null import name");
                    }
                    self.userCurrentImport.name = value;
                }
            }

            @Override
            public void writeInt(final IonManagedBinaryWriter self, final long value)
            {
                if (self.user.getDepth() == 3)
                {
                    if (value > Integer.MAX_VALUE || value < 1)
                    {
                        throw new IllegalArgumentException("Invalid integer value in import: " + value);
                    }
                    switch (self.user.getFieldId())
                    {
                        case VERSION_SID:
                            self.userCurrentImport.version = (int) value;
                            break;
                        case MAX_ID_SID:
                            self.userCurrentImport.maxId = (int) value;
                            break;
                    }
                }
            }
        },
        // TODO deal with the case that nonsense is written into the list
        LOCALS_AT_SYMBOLS
        {
            @Override
            public void beforeStepIn(final IonManagedBinaryWriter self, final IonType type) {}

            @Override
            public void afterStepOut(final IonManagedBinaryWriter self) {
                if (self.user.getDepth() == 1)
                {
                    self.userState = LOCALS_AT_TOP;
                }
            }

            @Override
            public void writeString(final IonManagedBinaryWriter self, String value)
            {
                if (self.user.getDepth() == 2)
                {
                    self.userSymbols.add(value);
                }
            }
        };

        public abstract void beforeStepIn(final IonManagedBinaryWriter self, final IonType type) throws IOException;
        public abstract void afterStepOut(final IonManagedBinaryWriter self) throws IOException;

        public void writeString(final IonManagedBinaryWriter self, final String value) throws IOException {}
        public void writeInt(final IonManagedBinaryWriter self, final long value) throws IOException {}
        public void writeInt(IonManagedBinaryWriter self, BigInteger value) throws IOException
        {
            // this will truncate if too big--but we don't care for interception
            writeInt(self, value.longValue());
        }
    }

    private static final SymbolTable[] EMPTY_SYMBOL_TABLE_ARRAY = new SymbolTable[0];

    /** View over the internal local symbol table state as a symbol table. */
    private class LocalSymbolTableView extends AbstractSymbolTable
    {
        public LocalSymbolTableView()
        {
            super(null, 0);
        }

        public Iterator<String> iterateDeclaredSymbolNames()
        {
            return locals.keySet().iterator();
        }

        public int getMaxId()
        {
            return getImportedMaxId() + locals.size();
        }

        public SymbolTable[] getImportedTables()
        {
            return imports.parents.toArray(EMPTY_SYMBOL_TABLE_ARRAY);
        }

        public int getImportedMaxId()
        {
            return imports.localSidStart - 1;
        }

        public boolean isSystemTable() { return false; }
        public boolean isSubstitute()  { return false; }
        public boolean isSharedTable() { return false; }
        public boolean isLocalTable()  { return true; }
        public boolean isReadOnly()    { return localsLocked; }

        public SymbolTable getSystemSymbolTable()
        {
            return systemSymbolTable();
        }

        public SymbolToken intern(final String text)
        {
            SymbolToken token = find(text);
            if (token == null)
            {
                if (localsLocked)
                {
                    throw new IonException("Cannot intern into locked (read-only) local symbol table");
                }
                token = IonManagedBinaryWriter.this.intern(text);
            }
            return token;
        }

        public String findKnownSymbol(final int id)
        {
            for (final SymbolTable table : imports.parents)
            {
                final String text = table.findKnownSymbol(id);
                if (text != null)
                {
                    return text;
                }
            }
            // TODO decide if it is worth making this better than O(N)
            //      requires more state tracking (but for what use case?)
            for (final SymbolToken token : locals.values())
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
            return locals.get(text);
        }

        @Override
        public void makeReadOnly()
        {
            localsLocked = true;
        }
    }

    private final IonCatalog                    catalog;
    private final ImportedSymbolContext         bootstrapImports;

    private ImportedSymbolContext               imports;
    private final Map<String, SymbolToken>      locals;
    private boolean                             localsLocked;
    private SymbolTable                         localSymbolTableView;

    private final IonRawBinaryWriter            symbols;
    private final IonRawBinaryWriter            user;

    private UserState                           userState;
    private SymbolState                         symbolState;

    // local symbol table management for when user writes a local symbol table through us
    private long                                userSymbolTablePosition;
    private final List<SymbolTable>             userImports;
    private final List<String>                  userSymbols;
    private final ImportDescriptor              userCurrentImport;

    private boolean                             forceSystemOutput;
    private boolean                             closed;

    /*package*/ IonManagedBinaryWriter(final PrivateIonManagedBinaryWriterBuilder builder,
                                       final OutputStream out)
                                       throws IOException
    {
        super(builder.optimization);
        this.symbols = new IonRawBinaryWriter(
            builder.provider,
            builder.symbolsBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer
            StreamCloseMode.NO_CLOSE,
            StreamFlushMode.NO_FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );
        this.user = new IonRawBinaryWriter(
            builder.provider,
            builder.userBlockSize,
            out,
            WriteValueOptimization.NONE, // optimization is not relevant for the nested raw writer
            StreamCloseMode.CLOSE,
            StreamFlushMode.FLUSH,
            builder.preallocationMode,
            builder.isFloatBinary32Enabled
        );

        this.catalog = builder.catalog;
        this.bootstrapImports = builder.imports;

        this.locals = new LinkedHashMap<String, SymbolToken>();
        this.localsLocked = false;
        this.localSymbolTableView = new LocalSymbolTableView();
        this.symbolState = SymbolState.SYSTEM_SYMBOLS;

        this.forceSystemOutput = false;
        this.closed = false;

        this.userState = UserState.NORMAL;

        this.userSymbolTablePosition = 0L;
        this.userImports = new ArrayList<SymbolTable>();
        this.userSymbols = new ArrayList<String>();
        this.userCurrentImport = new ImportDescriptor();

        // TODO decide if initial LST should survive finish() and seed the next LST
        final SymbolTable lst = builder.initialSymbolTable;
        if (lst != null)
        {
            // build import context from seeded LST
            final List<SymbolTable> lstImportList = Arrays.asList(lst.getImportedTables());
            // TODO determine if the resolver mode should be configurable for this use case
            final ImportedSymbolContext lstImports = new ImportedSymbolContext(ImportedSymbolResolverMode.DELEGATE, lstImportList);
            this.imports = lstImports;

            // intern all of the local symbols provided from LST
            final Iterator<String> symbolIter = lst.iterateDeclaredSymbolNames();
            while (symbolIter.hasNext())
            {
                final String text = symbolIter.next();
                intern(text);
            }

            // TODO determine if we really need to force emitting LST if there are no imports/locals
            startLocalSymbolTableIfNeeded(/*writeIVM*/ true);
        }
        else
        {
            this.imports = builder.imports;
        }
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

    public void writeIonVersionMarker() throws IOException
    {
        // this has to force a reset of symbol table context
        finish();
    }

    public int getDepth()
    {
        return user.getDepth();
    }

    // Symbol Table Management

    private void startLocalSymbolTableIfNeeded(final boolean writeIVM) throws IOException
    {
        if (symbolState == SymbolState.SYSTEM_SYMBOLS)
        {
            if (writeIVM)
            {
                symbols.writeIonVersionMarker();
            }
            symbols.addTypeAnnotationSymbol(systemSymbol(ION_SYMBOL_TABLE_SID));
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
            symbolState = SymbolState.LOCAL_SYMBOLS_WITH_IMPORTS_ONLY;
        }
    }

    private void startLocalSymbolTableSymbolListIfNeeded() throws IOException
    {
        if (symbolState == SymbolState.LOCAL_SYMBOLS_WITH_IMPORTS_ONLY)
        {
            symbols.setFieldNameSymbol(systemSymbol(SYMBOLS_SID));
            symbols.stepIn(LIST);
            // XXX no step out

            symbolState = SymbolState.LOCAL_SYMBOLS;
        }
    }

    private SymbolToken intern(final String text)
    {
        if (text == null)
        {
            return null;
        }
        if ("".equals(text))
        {
            throw new EmptySymbolException();
        }
        try
        {
            SymbolToken token = imports.importedSymbols.get(text);
            if (token != null)
            {
                if (token.getSid() > ION_1_0_MAX_ID)
                {
                    // using a symbol from an import triggers emitting locals
                    startLocalSymbolTableIfNeeded(/*writeIVM*/ true);
                }
                return token;
            }
            // try the locals
            token = locals.get(text);
            if (token == null)
            {
                if (localsLocked)
                {
                    throw new IonException("Local symbol table was locked (made read-only)");
                }

                // if we got here, this is a new symbol and we better start up the locals
                startLocalSymbolTableIfNeeded(/*writeIVM*/ true);
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

    private SymbolToken intern(final SymbolToken token)
    {
        if (token == null)
        {
            return null;
        }
        final String text = token.getText();
        if (text != null)
        {
            // string content always makes us intern
            return intern(text);
        }
        // no text, we just return what we got
        return token;
    }

    public SymbolTable getSymbolTable()
    {
        if (symbolState == SymbolState.SYSTEM_SYMBOLS && imports.parents.isEmpty())
        {
            return Symbols.systemSymbolTable();
        }

        // TODO this returns a symbol table view that gets truncated across reset boundaries (e.g. IVM/LST definitions)
        //      we need to figure out, what the actual API contract is, because this *probably* violates the expectation of the caller.

        return localSymbolTableView;
    }

    // Current Value Meta

    public void setFieldName(final String name)
    {
        if (!isInStruct())
        {
            throw new IllegalStateException("IonWriter.setFieldName() must be called before writing a value into a struct.");
        }
        if (name == null)
        {
            throw new NullPointerException("Null field name is not allowed.");
        }
        final SymbolToken token = intern(name);
        user.setFieldNameSymbol(token);
    }

    public void setFieldNameSymbol(SymbolToken token)
    {
        token = intern(token);
        user.setFieldNameSymbol(token);
    }

    public void setTypeAnnotations(final String... annotations)
    {
        if (annotations == null)
        {
            user.setTypeAnnotationSymbols((SymbolToken[]) null);
        }
        else
        {
            final SymbolToken[] tokens = new SymbolToken[annotations.length];
            for (int i = 0; i < tokens.length; i++)
            {
                tokens[i] = intern(annotations[i]);
            }
            user.setTypeAnnotationSymbols(tokens);
        }
    }

    public void setTypeAnnotationSymbols(final SymbolToken... annotations)
    {
        if (annotations == null)
        {
            user.setTypeAnnotationSymbols((SymbolToken[]) null);
        }
        else
        {
            for (int i = 0; i < annotations.length; i++)
            {
                annotations[i] = intern(annotations[i]);
            }
            user.setTypeAnnotationSymbols(annotations);
        }
    }

    public void addTypeAnnotation(final String annotation)
    {
        final SymbolToken token = intern(annotation);
        user.addTypeAnnotationSymbol(token);
    }

    // Container Manipulation

    public void stepIn(final IonType containerType) throws IOException
    {
        userState.beforeStepIn(this, containerType);
        user.stepIn(containerType);
    }

    public void stepOut() throws IOException
    {
        user.stepOut();
        userState.afterStepOut(this);
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
        userState.writeInt(this, value);
        user.writeInt(value);
    }

    public void writeInt(final BigInteger value) throws IOException
    {
        userState.writeInt(this, value);
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
        writeSymbolToken(token);
    }

    public void writeSymbolToken(SymbolToken token) throws IOException
    {
        token = intern(token);
        if (token != null && token.getSid() == ION_1_0_SID && user.getDepth() == 0 && !user.hasAnnotations())
        {
            if (user.hasWrittenValuesSinceFinished())
            {
                // this explicitly translates SID 2 to an IVM and flushes out local symbol state
                finish();
            }
            else
            {
                // TODO determine if redundant IVM writes need to actually be surfaced
                // we need to signal that we need to write out the IVM even if nothing else is written
                forceSystemOutput = true;
            }
            return;
        }
        user.writeSymbolToken(token);
    }

    public void writeString(final String value) throws IOException
    {
        userState.writeString(this, value);
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

    public void writeBytes(byte[] data, int off, int len) throws IOException
    {
        // this is a raw transfer--we basically have to dump the symbol table since we don't have much context
        startLocalSymbolTableIfNeeded(/*writeIVM*/ true);
        user.writeBytes(data, off, len);
    }

    // Stream Terminators

    public void flush() throws IOException
    {
        if (getDepth() == 0 && localsLocked)
        {
            unsafeFlush();
        }
    }

    private void unsafeFlush() throws IOException
    {
        if (user.hasWrittenValuesSinceFinished() || forceSystemOutput)
        {
            // this implies that we have a local symbol table of some sort and the user locked it
            symbolState.closeTable(symbols);
        }

        // make sure that until the local symbol state changes we no-op the table closing routine
        symbolState = SymbolState.LOCAL_SYMBOLS_FLUSHED;
        forceSystemOutput = false;
        // push the data out
        symbols.finish();
        user.finish();
    }

    public void finish() throws IOException
    {
        if (getDepth() != 0)
        {
            throw new IllegalStateException("IonWriter.finish() can only be called at top-level.");
        }
        unsafeFlush();
        // Reset local symbols
        // TODO be more configurable with respect to local symbol table caching
        locals.clear();
        localsLocked = false;
        symbolState = SymbolState.SYSTEM_SYMBOLS;
        imports = bootstrapImports;
    }

    public void close() throws IOException
    {
        if (closed)
        {
            return;
        }
        closed = true;
        try
        {
            finish();
        }
        catch (IllegalStateException e)
        {
            // callers do not expect this...
        }
        finally
        {
            try
            {
                symbols.close();
            }
            finally
            {
                user.close();
            }
        }
    }
}
