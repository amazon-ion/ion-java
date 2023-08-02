package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoder;
import com.amazon.ion.impl.bin.utf8.Utf8StringDecoderPool;
import com.amazon.ion.system.SimpleCatalog;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * <p>
 * This implementation differs from the existing non-incremental binary reader implementation in that if
 * {@link IonReader#next()} returns {@code null} at the top-level, it indicates that there is not (yet) enough data in
 * the stream to complete a top-level value. The user may wait for more data to become available in the stream and
 * call {@link IonReader#next()} again to continue reading. Unlike the non-incremental reader, the incremental reader
 * will never throw an exception due to unexpected EOF during {@code next()}. If, however, {@link IonReader#close()} is
 * called when an incomplete value is buffered, an {@link IonException} will be raised.
 * </p>
 * <p>
 * Although the incremental binary reader implementation provides performance superior to the non-incremental reader
 * implementation for both incremental and non-incremental use cases, there is one caveat: the incremental
 * implementation must be able to buffer an entire top-level value and any preceding system values (Ion version
 * marker(s) and symbol table(s)) in memory. This means that each value and preceding system values must be no larger
 * than any of the following:
 * <ul>
 * <li>The configured maximum buffer size of the {@link IonBufferConfiguration}.</li>
 * <li>The memory available to the JVM.</li>
 * <li>2GB, because the buffer is held in a Java {@code byte[]}, which is indexed by an {@code int}.</li>
 * </ul>
 * This will not be a problem for the vast majority of Ion streams, as it is
 * rare for a single top-level value or symbol table to exceed a few megabytes in size. However, if the size of the
 * stream's values risk exceeding the available memory, then this implementation must not be used.
 * </p>
 * <p>
 * To enable this implementation, use {@code IonReaderBuilder.withIncrementalReadingEnabled(true)}.
 * </p>
 */
class IonReaderBinaryIncremental implements IonReader, _Private_ReaderWriter, _Private_IncrementalReader {

    /*
     * Potential future enhancements:
     * - Split this implementation into a user-level reader and a system-level reader, like the existing implementation.
     *   This allows this implementation to be used when the user requests a system reader.
     * - Do not require buffering an entire top-level value. This would be a pretty major overhaul. It may be possible
     *   to implement using different buffers for each depth. Doing this may also make it possible to avoid buffering
     *   a value (at any depth) until stepIn() or *Value() is called on it, enabling faster skip-scanning.
     * - Allow for this implementation to produce the same non-incremental behavior as the old implementation; namely,
     *   that running out of data during next() would raise an IonException. See the note in the implementation of
     *   close() below. Implementing this bullet and the previous two bullets would allow us to remove the old binary
     *   IonReader implementation.
     * - Add a builder/constructor option that uses a user-provided byte[] directly. This would allow data to be read
     *   in-place without the need to copy to a separate buffer. Non-incremental behavior (as described in the previous
     *   bullet) is likely a requirement of this feature.
     * - System symbol table configuration needs to be generalized to support future Ion versions. See the constructor,
     *   resetSymbolTable(), and resetImports().
     * - When accessed via an iterator, annotations can be parsed incrementally instead of parsing the entire sequence
     *   up-front.
     * - Provide users the option to spawn a thread that pre-buffers the next value. There would be two buffers: one
     *   for the user thread, and one for the pre-fetching thread. They are swapped every time the user calls next().
     */

    /**
     * Holds the information that the binary reader must keep track of for containers at any depth.
     */
    private static class ContainerInfo {

        /**
         * The container's type.
         */
        private IonType type;

        /**
         * The byte position of the end of the container.
         */
        private int endPosition;

        private ContainerInfo initialize(final IonType type, final int endPosition) {
            this.type = type;
            this.endPosition = endPosition;
            return this;
        }
    }

    /**
     * The standard {@link IonBufferConfiguration}. This will be used unless the user chooses custom settings.
     */
    private static final IonBufferConfiguration STANDARD_BUFFER_CONFIGURATION =
        IonBufferConfiguration.Builder.standard().build();

    // Symbol IDs for symbols contained in the system symbol table.
    private static class SystemSymbolIDs {

        // The system symbol table SID for the text "$ion_symbol_table".
        private static final int ION_SYMBOL_TABLE_ID = 3;

        // The system symbol table SID for the text "name".
        private static final int NAME_ID = 4;

        // The system symbol table SID for the text "version".
        private static final int VERSION_ID = 5;

        // The system symbol table SID for the text "imports".
        private static final int IMPORTS_ID = 6;

        // The system symbol table SID for the text "symbols".
        private static final int SYMBOLS_ID = 7;

        // The system symbol table SID for the text "max_id".
        private static final int MAX_ID_ID = 8;
    }

    /**
     * @param value a non-negative number.
     * @return the exponent of the next power of two greater than the given number.
     */
    private static int logBase2(int value) {
        return 32 - Integer.numberOfLeadingZeros(value == 0 ? 0 : value - 1);
    }

    /**
     * Cache of configurations for fixed-sized streams. FIXED_SIZE_CONFIGURATIONS[i] returns a configuration with
     * buffer size max(8, 2^i). Retrieve a configuration large enough for a given size using
     * FIXED_SIZE_CONFIGURATIONS(logBase2(size)). Only supports sizes less than or equal to
     * STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize().
     */
    private static final IonBufferConfiguration[] FIXED_SIZE_CONFIGURATIONS;

    static {
        int maxBufferSizeExponent = logBase2(STANDARD_BUFFER_CONFIGURATION.getInitialBufferSize());
        FIXED_SIZE_CONFIGURATIONS = new IonBufferConfiguration[maxBufferSizeExponent + 1];
        for (int i = 0; i <= maxBufferSizeExponent; i++) {
            // Create a buffer configuration for buffers of size 2^i. The minimum size is 8: the smallest power of two
            // larger than the minimum buffer size allowed.
            int size = Math.max(8, (int) Math.pow(2, i));
            FIXED_SIZE_CONFIGURATIONS[i] = IonBufferConfiguration.Builder.from(STANDARD_BUFFER_CONFIGURATION)
                .withInitialBufferSize(size)
                .withMaximumBufferSize(size)
                .build();
        }
    }

    // The final byte of the binary IVM.
    private static final int IVM_FINAL_BYTE = 0xEA;

    // Isolates the highest bit in a byte.
    private static final int HIGHEST_BIT_BITMASK = 0x80;

    // Isolates the lowest seven bits in a byte.
    private static final int LOWER_SEVEN_BITS_BITMASK = 0x7F;

    // Isolates the lowest six bits in a byte.
    private static final int LOWER_SIX_BITS_BITMASK = 0x3F;

    // The number of significant bits in each UInt byte.
    private static final int VALUE_BITS_PER_UINT_BYTE = 8;

    // The number of significant bits in each VarUInt byte.
    private static final int VALUE_BITS_PER_VARUINT_BYTE = 7;

    // An IonCatalog containing zero shared symbol tables.
    private static final IonCatalog EMPTY_CATALOG = new SimpleCatalog();

    // Initial capacity of the stack used to hold ContainerInfo. Each additional level of nesting in the data requires
    // a new ContainerInfo. Depths greater than 8 will be rare.
    private static final int CONTAINER_STACK_INITIAL_CAPACITY = 8;

    // Initial capacity of the ArrayList used to hold the symbol IDs of the annotations on the current value.
    private static final int ANNOTATIONS_LIST_INITIAL_CAPACITY = 8;

    // Initial capacity of the ArrayList used to hold the text in the current symbol table.
    private static final int SYMBOLS_LIST_INITIAL_CAPACITY = 128;

    // Single byte negative zero, represented as a VarInt. Often used in timestamp encodings to indicate unknown local
    // offset.
    private static final int VAR_INT_NEGATIVE_ZERO = 0xC0;

    // The number of bytes occupied by a Java int.
    private static final int INT_SIZE_IN_BYTES = 4;

    // The number of bytes occupied by a Java long.
    private static final int LONG_SIZE_IN_BYTES = 8;

    // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MIN_LONG = 0x80;

    // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
    private static final int MOST_SIGNIFICANT_BYTE_OF_MAX_LONG = 0x7F;

    // The second-most significant bit in the most significant byte of a VarInt is the sign.
    private static final int VAR_INT_SIGN_BITMASK = 0x40;

    // 32-bit floats must declare length 4.
    private static final int FLOAT_32_BYTE_LENGTH = 4;

    // The imports for Ion 1.0 data with no shared user imports.
    private static final LocalSymbolTableImports ION_1_0_IMPORTS
        = new LocalSymbolTableImports(SharedSymbolTable.getSystemSymbolTable(1));

    // The InputStream that provides the binary Ion data.
    private final InputStream inputStream;

    // Wrapper for the InputStream that ensures an entire top-level value is available.
    private final IonReaderLookaheadBuffer lookahead;

    // Buffer that stores top-level values.
    private final ResizingPipedInputStream buffer;

    // Converter between scalar types, allowing, for example, for a value encoded as an Ion float to be returned as a
    // Java `long` via `IonReader.longValue()`.
    private final _Private_ScalarConversions.ValueVariant scalarConverter;

    // Stack to hold container info. Stepping into a container results in a push; stepping out results in a pop.
    private final _Private_RecyclingStack<ContainerInfo> containerStack;

    private final Utf8StringDecoder utf8Decoder = Utf8StringDecoderPool.getInstance().getOrCreate();

    // The symbol IDs for the annotations on the current value.
    private final IntList annotationSids;

    // True if the annotation iterator will be reused across values; otherwise, false.
    private final boolean isAnnotationIteratorReuseEnabled;

    // Reusable iterator over the annotations on the current value.
    private final AnnotationIterator annotationIterator;

    // The text representations of the symbol table that is currently in scope, indexed by symbol ID. If the element at
    // a particular index is null, that symbol has unknown text.
    private final List<String> symbols;

    // The catalog used by the reader to resolve shared symbol table imports.
    private final IonCatalog catalog;

    // The shared symbol tables imported by the local symbol table that is currently in scope.
    private LocalSymbolTableImports imports = ION_1_0_IMPORTS;

    // A map of symbol ID to SymbolToken representation. Because most use cases only require symbol text, this
    // is used only if necessary to avoid imposing the extra expense on all symbol lookups.
    private List<SymbolToken> symbolTokensById = null;

    // The cached SymbolTable representation of the current local symbol table. Invalidated whenever a local
    // symbol table is encountered in the stream.
    private SymbolTable cachedReadOnlySymbolTable = null;

    // The SymbolTable that was transferred via the last call to pop_passed_symbol_table.
    private SymbolTable symbolTableLastTransferred = null;

    // The symbol ID of the current value's field name, or -1 if the current value is not in a struct.
    private int fieldNameSid = -1;

    // The major version of the Ion encoding currently being read.
    private int majorVersion = 1;

    // The minor version of the Ion encoding currently being read.
    private int minorVersion = 0;

    // The number of bytes of a lob value that the user has consumed, allowing for piecewise reads.
    private int lobBytesRead = 0;

    // The type of value at which the reader is currently positioned.
    private IonType valueType = null;

    // Information about the type ID byte for the value at which the reader is currently positioned.
    private IonTypeID valueTypeID = null;

    // Indicates whether there are annotations on the current value.
    private boolean hasAnnotations = false;

    // Indicates whether a complete top-level value is currenty buffered.
    private boolean completeValueBuffered = false;

    // --- Byte position markers ---
    // Note: absolute positions/indexes can be used because the bytes that represent a single top-level value are
    // always handled in two sequential phases: first, the bytes are buffered, and then they are read. These operations
    // will never be interleaved during the processing of a single value. As a result, the underlying buffer
    // will always hold all of the bytes for a single top-level value in a contiguous sequence, even if the buffer
    // has to grow to hold all of the value's bytes.

    // The buffer position of the first byte of the value representation (after the type ID and optional length field).
    private int valueStartPosition = -1;

    // The buffer position of the byte after the last byte in the value representation.
    private int valueEndPosition = -1;

    // The buffer position of the first byte of the annotation wrapper for the current value.
    private int annotationStartPosition = -1;

    // The buffer position of the byte after the last byte in the annotation wrapper for the current value.
    private int annotationEndPosition = -1;

    // The index of the next byte to peek from the underlying buffer.
    private int peekIndex = -1;

    // ------

    /**
     * Constructor.
     * @param builder the builder containing the configuration for the new reader.
     * @param inputStream the InputStream that provides binary Ion data.
     */
    IonReaderBinaryIncremental(IonReaderBuilder builder, InputStream inputStream) {
        this.inputStream = inputStream;
        this.catalog = builder.getCatalog() == null ? EMPTY_CATALOG : builder.getCatalog();
        if (builder.isAnnotationIteratorReuseEnabled()) {
            isAnnotationIteratorReuseEnabled = true;
            annotationIterator = new AnnotationIterator();
        } else {
            isAnnotationIteratorReuseEnabled = false;
            annotationIterator = null;
        }
        IonBufferConfiguration configuration = builder.getBufferConfiguration();
        if (configuration == null) {
            configuration = STANDARD_BUFFER_CONFIGURATION;
            if (inputStream instanceof ByteArrayInputStream) {
                // ByteArrayInputStreams are fixed-size streams. Clamp the reader's internal buffer size at the size of
                // the stream to avoid wastefully allocating extra space that will never be needed. It is still
                // preferable for the user to manually specify the buffer size if it's less than the default, as doing
                // so allows this branch to be skipped.
                int fixedBufferSize;
                try {
                    fixedBufferSize = inputStream.available();
                } catch (IOException e) {
                    // ByteArrayInputStream.available() does not throw.
                    throw new IllegalStateException(e);
                }
                if (configuration.getInitialBufferSize() > fixedBufferSize) {
                    configuration = FIXED_SIZE_CONFIGURATIONS[logBase2(fixedBufferSize)];
                }
            }
        }
        lookahead = new IonReaderLookaheadBuffer(configuration, inputStream);
        buffer = (ResizingPipedInputStream) lookahead.getPipe();
        containerStack = new _Private_RecyclingStack<>(
                CONTAINER_STACK_INITIAL_CAPACITY,
                ContainerInfo::new
        );

        annotationSids = new IntList(ANNOTATIONS_LIST_INITIAL_CAPACITY);
        symbols = new ArrayList<>(SYMBOLS_LIST_INITIAL_CAPACITY);
        scalarConverter = new _Private_ScalarConversions.ValueVariant();
        resetImports();
    }

    /**
     * Reusable iterator over the annotations on the current value.
     */
    private class AnnotationIterator implements Iterator<String> {

        // The byte position of the annotation to return from the next call to next().
        private int nextAnnotationPeekIndex;

        @Override
        public boolean hasNext() {
            return nextAnnotationPeekIndex < annotationEndPosition;
        }

        @Override
        public String next() {
            int savedPeekIndex = peekIndex;
            peekIndex = nextAnnotationPeekIndex;
            int sid = readVarUInt();
            nextAnnotationPeekIndex = peekIndex;
            peekIndex = savedPeekIndex;
            String annotation = getSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            return annotation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support element removal.");
        }

        /**
         * Prepare the iterator to iterate over the annotations on the current value.
         */
        void ready() {
            nextAnnotationPeekIndex = annotationStartPosition;
        }

        /**
         * Invalidate the iterator so that all future calls to {@link #hasNext()} will return false until the
         * next call to {@link #ready()}.
         */
        void invalidate() {
            nextAnnotationPeekIndex = Integer.MAX_VALUE;
        }
    }

    /**
     * Non-reusable iterator over the annotations on the current value. May be iterated even if the reader advances
     * past the current value.
     */
    private class SingleUseAnnotationIterator implements Iterator<String> {

        // All of the annotation SIDs on the current value.
        private final IntList annotationSids;
        // The index into `annotationSids` containing the next annotation to be returned.
        private int index = 0;

        SingleUseAnnotationIterator() {
            annotationSids = new IntList(getAnnotationSids());
        }

        @Override
        public boolean hasNext() {
            return index < annotationSids.size();
        }

        @Override
        public String next() {
            int sid = annotationSids.get(index);
            String annotation = getSymbol(sid);
            if (annotation == null) {
                throw new UnknownSymbolException(sid);
            }
            index++;
            return annotation;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support element removal.");
        }
    }

    /**
     * SymbolToken implementation that includes ImportLocation.
     */
    static class SymbolTokenImpl implements _Private_SymbolToken {

        // The symbol's text, or null if the text is unknown.
        private final String text;

        // The local symbol ID of this symbol within a particular local symbol table.
        private final int sid;

        // The import location of the symbol (only relevant if the text is unknown).
        private final ImportLocation importLocation;

        SymbolTokenImpl(String text, int sid, ImportLocation importLocation) {
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
            return Objects.equals(getText(), other.getText());
        }

        @Override
        public int hashCode() {
            if(getText() != null) return getText().hashCode();
            return 0;
        }
    }

    /**
     * Gets the system symbol table for the Ion version currently active.
     * @return a system SymbolTable.
     */
    private SymbolTable getSystemSymbolTable() {
        // Note: Ion 1.1 currently proposes changes to the system symbol table. If this is finalized, then
        // 'majorVersion' cannot be used to look up the system symbol table; both 'majorVersion' and 'minorVersion'
        // will need to be used.
        return SharedSymbolTable.getSystemSymbolTable(majorVersion);
    }

    /**
     * Read-only snapshot of the local symbol table at the reader's current position.
     */
    private class LocalSymbolTableSnapshot implements SymbolTable, SymbolTableAsStruct {

        // The system symbol table.
        private final SymbolTable system = IonReaderBinaryIncremental.this.getSystemSymbolTable();

        // The max ID of this local symbol table.
        private final int maxId;

        // The shared symbol tables imported by this local symbol table.
        private final LocalSymbolTableImports importedTables;

        // Map representation of this symbol table. Keys are symbol text; values are the lowest symbol ID that maps
        // to that text.
        final Map<String, Integer> mapView;

        // List representation of this symbol table, indexed by symbol ID.
        final List<String> listView;

        private SymbolTableStructCache structCache = null;

        LocalSymbolTableSnapshot() {
            int importsMaxId = imports.getMaxId();
            int numberOfLocalSymbols = symbols.size();
            // Note: 'imports' is immutable, so a clone is not needed.
            importedTables = imports;
            maxId = importsMaxId + numberOfLocalSymbols;
            // Map with initial size the number of symbols and load factor 1, meaning it must be full before growing.
            // It is not expected to grow.
            listView = new ArrayList<>(symbols.subList(0, numberOfLocalSymbols));
            mapView = new HashMap<>((int) Math.ceil(numberOfLocalSymbols / 0.75), 0.75f);
            for (int i = 0; i < numberOfLocalSymbols; i++) {
                String symbol = listView.get(i);
                if (symbol != null) {
                    mapView.put(symbol, i + importsMaxId + 1);
                }
            }
        }

        @Override
        public String getName() {
            return null;
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public boolean isLocalTable() {
            return true;
        }

        @Override
        public boolean isSharedTable() {
            return false;
        }

        @Override
        public boolean isSubstitute() {
            return false;
        }

        @Override
        public boolean isSystemTable() {
            return false;
        }

        @Override
        public SymbolTable getSystemSymbolTable() {
            return system;
        }

        @Override
        public String getIonVersionId() {
            return system.getIonVersionId();
        }

        @Override
        public SymbolTable[] getImportedTables() {
            return importedTables.getImportedTables();
        }

        @Override
        public int getImportedMaxId() {
            return importedTables.getMaxId();
        }

        @Override
        public SymbolToken find(String text) {
            SymbolToken token = importedTables.find(text);
            if (token != null) {
                return token;
            }
            Integer sid = mapView.get(text);
            if (sid == null) {
                return null;
            }
            // The following per-call allocation is intentional. When weighed against the alternative of making
            // 'mapView' a 'Map<String, SymbolToken>` instead of a `Map<String, Integer>`, the following points should
            // be considered:
            // 1. A LocalSymbolTableSnapshot is only created when getSymbolTable() is called on the reader. The reader
            // does not use the LocalSymbolTableSnapshot internally. There are two cases when getSymbolTable() would be
            // called: a) when the user calls it, which will basically never happen, and b) when the user uses
            // IonSystem.iterate over the reader, in which case each top-level value holds a reference to the symbol
            // table that was in scope when it occurred. In case a), in addition to rarely being called at all, it
            // would be even rarer for a user to use find() to retrieve each symbol (especially more than once) from the
            // returned symbol table. Case b) may be called more frequently, but it remains equally rare that a user
            // would retrieve each symbol at least once.
            // 2. If we make mapView a Map<String, SymbolToken>, then we are guaranteeing that we will allocate at least
            // one SymbolToken per symbol (because mapView is created in the constructor of LocalSymbolTableSnapshot)
            // even though it's unlikely most will ever be needed.
            return new SymbolTokenImpl(text, sid, null);
        }

        @Override
        public int findSymbol(String name) {
            Integer sid = importedTables.findSymbol(name);
            if (sid > UNKNOWN_SYMBOL_ID) {
                return sid;
            }
            sid = mapView.get(name);
            if (sid == null) {
                return UNKNOWN_SYMBOL_ID;
            }
            return sid;
        }

        @Override
        public String findKnownSymbol(int id) {
            if (id < 0) {
                throw new IllegalArgumentException("Symbol IDs must be at least 0.");
            }
            if (id > getMaxId()) {
                return null;
            }
            return IonReaderBinaryIncremental.this.getSymbolString(id, importedTables, listView);
        }

        @Override
        public Iterator<String> iterateDeclaredSymbolNames() {
            return new Iterator<String>() {

                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < listView.size();
                }

                @Override
                public String next() {
                    String symbol = listView.get(index);
                    index++;
                    return symbol;
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException("This iterator does not support element removal.");
                }
            };
        }

        @Override
        public SymbolToken intern(String text) {
            SymbolToken token = find(text);
            if (token != null) {
                return token;
            }
            throw new ReadOnlyValueException();
        }

        @Override
        public int getMaxId() {
            return maxId;
        }

        @Override
        public boolean isReadOnly() {
            return true;
        }

        @Override
        public void makeReadOnly() {
            // The symbol table is already read-only.
        }

        @Override
        public void writeTo(IonWriter writer) throws IOException {
            IonReader reader = new SymbolTableReader(this);
            writer.writeValues(reader);
        }

        @Override
        public String toString() {
            return "(LocalSymbolTable max_id:" + getMaxId() + ')';
        }

        @Override
        public IonStruct getIonRepresentation(ValueFactory valueFactory) {
            if (structCache == null) {
                structCache = new SymbolTableStructCache(this, getImportedTables(), null);
            }
            return structCache.getIonRepresentation(valueFactory);
        }
    }

    /**
     * Throw if the reader is attempting to process an Ion version that it does not support.
     */
    private void requireSupportedIonVersion() {
        if (majorVersion != 1 || minorVersion != 0) {
            throw new IonException(String.format("Unsupported Ion version: %d.%d", majorVersion, minorVersion));
        }
    }

    /**
     * Reset the local symbol table to the system symbol table.
     */
    private void resetSymbolTable() {
        // Note: when there is a new version of Ion, check majorVersion and minorVersion here and set the appropriate
        // system symbol table.
        symbols.clear();
        cachedReadOnlySymbolTable = null;
        if (symbolTokensById != null) {
            symbolTokensById.clear();
        }
    }

    /**
     * Resets the value's annotations.
     */
    private void resetAnnotations() {
        hasAnnotations = false;
        if (isAnnotationIteratorReuseEnabled) {
            annotationIterator.invalidate();
        }
    }

    /**
     * Clear the list of imported shared symbol tables.
     */
    private void resetImports() {
        // Note: when support for the next version of Ion is added, conditionals on 'majorVersion' and 'minorVersion'
        // must be added here.
        imports = ION_1_0_IMPORTS;
    }

    /**
     * Creates a shared symbol table import, resolving it from the catalog if possible.
     * @param name the name of the shared symbol table.
     * @param version the version of the shared symbol table.
     * @param maxId the max_id of the shared symbol table. This value takes precedence over the actual max_id for the
     *              shared symbol table at the requested version.
     */
    private SymbolTable createImport(String name, int version, int maxId) {
        SymbolTable shared = catalog.getTable(name, version);
        if (shared == null) {
            // No match. All symbol IDs that fall within this shared symbol table's range will have unknown text.
            return new SubstituteSymbolTable(name, version, maxId);
        } else if (shared.getMaxId() != maxId || shared.getVersion() != version) {
            // Partial match. If the requested max_id exceeds the actual max_id of the resolved shared symbol table,
            // symbol IDs that exceed the max_id of the resolved shared symbol table will have unknown text.
            return new SubstituteSymbolTable(shared, version, maxId);
        } else {
            // Exact match; the resolved shared symbol table may be used as-is.
            return shared;
        }
    }

    /**
     * Gets the String representation of the given symbol ID. It is the caller's responsibility to ensure that the
     * given symbol ID is within the max ID of the symbol table.
     * @param sid the symbol ID.
     * @param importedSymbols the symbol table's shared symbol table imports.
     * @param localSymbols the symbol table's local symbols.
     * @return a String, which will be null if the requested symbol ID has undefined text.
     */
    private String getSymbolString(int sid, LocalSymbolTableImports importedSymbols, List<String> localSymbols) {
        if (sid <= importedSymbols.getMaxId()) {
            return importedSymbols.findKnownSymbol(sid);
        }
        return localSymbols.get(sid - (importedSymbols.getMaxId() + 1));
    }

    /**
     * Calculates the symbol table's max ID.
     * @return the max ID.
     */
    private int maxSymbolId() {
        return symbols.size() + imports.getMaxId();
    }

    /**
     * Retrieves the String text for the given symbol ID.
     * @param sid a symbol ID.
     * @return a String.
     */
    private String getSymbol(int sid) {
        if (sid > maxSymbolId()) {
            throw new IonException("Symbol ID exceeds the max ID of the symbol table.");
        }
        return getSymbolString(sid, imports, symbols);
    }

    /**
     * Creates a SymbolToken representation of the given symbol ID.
     * @param sid a symbol ID.
     * @return a SymbolToken.
     */
    private SymbolToken getSymbolToken(int sid) {
        int symbolTableSize = maxSymbolId() + 1;
        if (symbolTokensById == null) {
            symbolTokensById = new ArrayList<>(symbolTableSize);
        }
        if (symbolTokensById.size() < symbolTableSize) {
            for (int i = symbolTokensById.size(); i < symbolTableSize; i++) {
                symbolTokensById.add(null);
            }
        }
        if (sid >= symbolTableSize) {
            throw new IonException("Symbol ID exceeds the max ID of the symbol table.");
        }
        SymbolToken token = symbolTokensById.get(sid);
        if (token == null) {
            String text = getSymbolString(sid, imports, symbols);
            ImportLocation importLocation = null;
            if (text == null) {
                // Note: this will never be a system symbol.
                if (sid > 0 && sid <= imports.getMaxId()) {
                    importLocation = imports.getImportLocation(sid);
                } else {
                    // All symbols with unknown text in the local symbol range are equivalent to symbol zero.
                    sid = 0;
                }
            }
            token = new SymbolTokenImpl(text, sid, importLocation);
            symbolTokensById.set(sid, token);
        }
        return token;
    }

    /**
     * Reads a local symbol table from the buffer.
     * @param marker marker for the start and end positions of the local symbol table in the buffer.
     */
    private void readSymbolTable(IonReaderLookaheadBuffer.Marker marker) {
        peekIndex = marker.startIndex;
        boolean isAppend = false;
        boolean hasSeenImports = false;
        boolean hasSeenSymbols = false;
        int symbolsPosition = -1;
        int symbolsEndPosition = -1;
        List<SymbolTable> newImports;
        while (peekIndex < marker.endIndex) {
            fieldNameSid = readVarUInt();
            IonTypeID typeID = readTypeId();
            calculateEndPosition(typeID);
            int currentValueEndPosition = valueEndPosition;
            if (fieldNameSid == SystemSymbolIDs.IMPORTS_ID) {
                if (hasSeenImports) {
                    throw new IonException("Symbol table contained multiple imports fields.");
                }
                if (typeID.type == IonType.SYMBOL) {
                    isAppend = readUInt(peekIndex, currentValueEndPosition) == SystemSymbolIDs.ION_SYMBOL_TABLE_ID;
                    peekIndex = currentValueEndPosition;
                } else if (typeID.type == IonType.LIST) {
                    resetImports();
                    newImports = new ArrayList<>(3);
                    newImports.add(getSystemSymbolTable());
                    stepIn();
                    IonType type = next();
                    while (type != null) {
                        String name = null;
                        int version = -1;
                        int maxId = -1;
                        if (type == IonType.STRUCT) {
                            stepIn();
                            type = next();
                            while (type != null) {
                                int fieldSid = getFieldId();
                                if (fieldSid == SystemSymbolIDs.NAME_ID) {
                                    if (type == IonType.STRING) {
                                        name = stringValue();
                                    }
                                } else if (fieldSid == SystemSymbolIDs.VERSION_ID) {
                                    if (type == IonType.INT) {
                                        version = intValue();
                                    }
                                } else if (fieldSid == SystemSymbolIDs.MAX_ID_ID) {
                                    if (type == IonType.INT) {
                                        maxId = intValue();
                                    }
                                }
                                type = next();
                            }
                            stepOut();
                        }
                        newImports.add(createImport(name, version, maxId));
                        type = next();
                    }
                    stepOut();
                    imports = new LocalSymbolTableImports(newImports);
                }
                if (!isAppend) {
                    // Clear the existing symbols before adding the new imported symbols.
                    resetSymbolTable();
                }
                hasSeenImports = true;
            } else if (fieldNameSid == SystemSymbolIDs.SYMBOLS_ID) {
                if (hasSeenSymbols) {
                    throw new IonException("Symbol table contained multiple symbols fields.");
                }
                if (typeID.type == IonType.LIST) {
                    // Just record this position and skip forward. Come back after the imports (if any) are parsed.
                    symbolsPosition = peekIndex;
                    symbolsEndPosition = currentValueEndPosition;
                }
                hasSeenSymbols = true;
            }
            peekIndex = currentValueEndPosition;
        }
        if (peekIndex > marker.endIndex) {
            throw new IonException("Malformed symbol table. Child values exceeded the length declared in the header.");
        }
        if (!hasSeenImports) {
            resetSymbolTable();
            resetImports();
        }
        if (symbolsPosition > -1) {
            peekIndex = symbolsPosition;
            valueType = IonType.LIST;
            valueEndPosition = symbolsEndPosition;
            stepIn();
            while (next() != null) {
                if (valueType != IonType.STRING) {
                    symbols.add(null);
                } else {
                    symbols.add(stringValue());
                }
            }
            stepOut();
            peekIndex = valueEndPosition;
        }
    }

    /**
     * Advance the reader to the next top-level value. Buffers an entire top-level value, reads any IVMs and/or local
     * symbol tables that precede the value, and sets the byte positions of important components of the value.
     */
    private void nextAtTopLevel() {
        if (completeValueBuffered) {
            // There is already data buffered, but the user is choosing to skip it.
            buffer.seekTo(valueEndPosition);
            completeValueBuffered = false;
        }
        try {
            lookahead.fillInput();
        } catch (Exception e) {
            throw new IonException(e);
        }
        if (lookahead.moreDataRequired()) {
            valueType = null;
            valueTypeID = null;
            return;
        }
        completeValueBuffered = true;
        if (lookahead.getIvmIndex() > -1) {
            peekIndex = lookahead.getIvmIndex();
            majorVersion = buffer.peek(peekIndex++);
            minorVersion = buffer.peek(peekIndex++);
            if (buffer.peek(peekIndex++) != IVM_FINAL_BYTE) {
                throw new IonException("Invalid Ion version marker.");
            }
            requireSupportedIonVersion();
            resetSymbolTable();
            resetImports();
            lookahead.resetIvmIndex();
        } else if (peekIndex < 0) {
            // peekIndex is initialized to -1 and only increases. This branch is reached if the IVM does not occur
            // first in the stream. This is necessary because currently a binary incremental reader will be created if
            // an empty stream is provided to the IonReaderBuilder. If, once bytes appear in the stream, those bytes do
            // not represent valid binary Ion, a quick failure is necessary.
            throw new IonException("Binary Ion must start with an Ion version marker.");
        }
        List<IonReaderLookaheadBuffer.Marker> symbolTableMarkers = lookahead.getSymbolTableMarkers();
        if (!symbolTableMarkers.isEmpty()) {
            // The cached SymbolTable (if any) is a snapshot in time, so it must be cleared whenever a new symbol
            // table is read regardless of whether the new LST is an append or a reset.
            cachedReadOnlySymbolTable = null;
            for (IonReaderLookaheadBuffer.Marker symbolTableMarker : symbolTableMarkers) {
                readSymbolTable(symbolTableMarker);
            }
            lookahead.resetSymbolTableMarkers();
        }
        peekIndex = lookahead.getValueStart();
        hasAnnotations = lookahead.hasAnnotations();
        if (hasAnnotations) {
            if (peekIndex >= lookahead.getValueEnd()) {
                throw new IonException("Annotation wrappers without values are invalid.");
            }
            annotationSids.clear();
            IonReaderLookaheadBuffer.Marker annotationSidsMarker = lookahead.getAnnotationSidsMarker();
            annotationStartPosition = annotationSidsMarker.startIndex;
            annotationEndPosition = annotationSidsMarker.endIndex;
            peekIndex = annotationEndPosition;
            valueTypeID = IonTypeID.TYPE_IDS[buffer.peek(peekIndex++)];
            int wrappedValueLength = valueTypeID.length;
            if (valueTypeID.variableLength) {
                wrappedValueLength = readVarUInt();
            }
            valueType = valueTypeID.type;
            if (valueType == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                throw new IonException("Nested annotations are invalid.");
            }
            if (peekIndex + wrappedValueLength != lookahead.getValueEnd()) {
                throw new IonException("Mismatched annotation wrapper length.");
            }
        } else {
            valueTypeID = lookahead.getValueTid();
            valueType = valueTypeID.type;
        }
        valueStartPosition = peekIndex;
        valueEndPosition = lookahead.getValueEnd();
        lookahead.resetNopPadIndex();
    }

    /**
     * Reads the type ID byte.
     * @return the TypeAndLength descriptor for the type ID byte.
     */
    private IonTypeID readTypeId() {
        valueTypeID = IonTypeID.TYPE_IDS[buffer.peek(peekIndex++)];
        if (!valueTypeID.isValid) {
            throw new IonException("Invalid type ID.");
        }
        valueType = valueTypeID.type;
        return valueTypeID;
    }

    /**
     * Calculates the end position for the given type ID descriptor.
     * @param typeID the type ID descriptor.
     */
    private void calculateEndPosition(IonTypeID typeID) {
        if (typeID.variableLength) {
            valueEndPosition = readVarUInt() + peekIndex;
        } else {
            valueEndPosition = typeID.length + peekIndex;
        }
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Marks the end of the current container by indicating that the reader is no longer positioned on a value.
     */
    private void endContainer() {
        valueType = null;
        valueTypeID = null;
        annotationStartPosition = -1;
        annotationEndPosition = -1;
        hasAnnotations = false;
    }

    /**
     * Advance the reader to the next value within a container, which must already be buffered.
     */
    private void nextBelowTopLevel() {
        // Seek past the previous value.
        if (peekIndex < valueEndPosition) {
            peekIndex = valueEndPosition;
        }
        if (peekIndex >= containerStack.peek().endPosition) {
            endContainer();
        } else {
            if (containerStack.peek().type == IonType.STRUCT) {
                fieldNameSid = readVarUInt();
            }
            IonTypeID typeID = readTypeId();
            while (typeID.isNopPad) {
                calculateEndPosition(typeID);
                peekIndex = valueEndPosition;
                if (peekIndex >= containerStack.peek().endPosition) {
                    endContainer();
                    return;
                }
                if (containerStack.peek().type == IonType.STRUCT) {
                    fieldNameSid = readVarUInt();
                }
                typeID = readTypeId();
            }
            calculateEndPosition(typeID);
            if (valueType == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                hasAnnotations = true;
                annotationSids.clear();
                int annotationsLength = readVarUInt();
                annotationStartPosition = peekIndex;
                annotationEndPosition = annotationStartPosition + annotationsLength;
                peekIndex = annotationEndPosition;
                typeID = readTypeId();
                if (typeID.isNopPad) {
                    throw new IonException(
                        "Invalid annotation wrapper: NOP pad may not occur inside an annotation wrapper."
                    );
                }
                if (valueType == IonTypeID.ION_TYPE_ANNOTATION_WRAPPER) {
                    throw new IonException("Nested annotations are invalid.");
                }
                long annotationWrapperEndPosition = valueEndPosition;
                calculateEndPosition(typeID);
                if (annotationWrapperEndPosition != valueEndPosition) {
                    throw new IonException(
                        "Invalid annotation wrapper: end of the wrapper did not match end of the value."
                    );
                }
            } else {
                annotationStartPosition = -1;
                annotationEndPosition = -1;
                hasAnnotations = false;
                if (valueEndPosition > containerStack.peek().endPosition) {
                    throw new IonException("Value overflowed its container.");
                }
            }
            if (!valueTypeID.isValid) {
                throw new IonException("Invalid type ID.");
            }
            valueStartPosition = peekIndex;
        }
    }

    @Override
    public IonType next() {
        fieldNameSid = -1;
        lobBytesRead = 0;
        valueStartPosition = -1;
        resetAnnotations();
        if (containerStack.isEmpty()) {
            nextAtTopLevel();
        } else {
            nextBelowTopLevel();
        }
        // Note: the following check is necessary to catch empty ordered structs, which are prohibited by the spec.
        // Unfortunately, this requires a check on every value for a condition that will probably never happen.
        if (
            valueType == IonType.STRUCT &&
            valueTypeID.lowerNibble == IonTypeID.ORDERED_STRUCT_NIBBLE &&
            valueStartPosition == valueEndPosition
        ) {
            throw new IonException("Ordered struct must not be empty.");
        }
        return valueType;
    }

    @Override
    public void stepIn() {
        if (!IonType.isContainer(valueType)) {
            throw new IonException("Must be positioned on a container to step in.");
        }
        // Note: the IonReader interface dictates that stepping into a null container has the same behavior as
        // an empty container.
        containerStack.push(c -> c.initialize(valueType, valueEndPosition));
        valueType = null;
        valueTypeID = null;
        valueEndPosition = -1;
        fieldNameSid = -1;
        valueStartPosition = -1;
    }

    @Override
    public void stepOut() {
        if (containerStack.isEmpty()) {
            // Note: this is IllegalStateException for consistency with the other binary IonReader implementation.
            throw new IllegalStateException("Cannot step out at top level.");
        }
        ContainerInfo containerInfo = containerStack.pop();
        valueEndPosition = containerInfo.endPosition;
        valueType = null;
        valueTypeID = null;
        fieldNameSid = -1;
        valueStartPosition = -1;
    }

    @Override
    public int getDepth() {
        return containerStack.size();
    }

    @Override
    public SymbolTable getSymbolTable() {
        if (cachedReadOnlySymbolTable == null) {
            if (symbols.size() == 0 && imports == ION_1_0_IMPORTS) {
                cachedReadOnlySymbolTable = imports.getSystemSymbolTable();
            } else {
                cachedReadOnlySymbolTable = new LocalSymbolTableSnapshot();
            }
        }
        return cachedReadOnlySymbolTable;
    }

    @Override
    public SymbolTable pop_passed_symbol_table() {
        SymbolTable currentSymbolTable = getSymbolTable();
        if (currentSymbolTable == symbolTableLastTransferred) {
            // This symbol table has already been returned. Since the contract is that it is a "pop", it should not
            // be returned twice.
            return null;
        }
        symbolTableLastTransferred = currentSymbolTable;
        return symbolTableLastTransferred;
    }

    @Override
    public IonType getType() {
        return valueType;
    }

    @Override
    public IntegerSize getIntegerSize() {
        if (valueType != IonType.INT || isNullValue()) {
            return null;
        }
        if (valueTypeID.length < INT_SIZE_IN_BYTES) {
            // Note: this is conservative. Most integers of size 4 also fit in an int, but since exactly the
            // same parsing code is used for ints and longs, there is no point wasting the time to determine the
            // smallest possible type.
            return IntegerSize.INT;
        } else if (valueTypeID.length < LONG_SIZE_IN_BYTES) {
            return IntegerSize.LONG;
        } else if (valueTypeID.length == LONG_SIZE_IN_BYTES) {
            // Because creating BigIntegers is so expensive, it is worth it to look ahead and determine exactly
            // which 8-byte integers can fit in a long.
            if (valueTypeID.isNegativeInt) {
                // The smallest negative 8-byte integer that can fit in a long is -0x80_00_00_00_00_00_00_00.
                int firstByte = buffer.peek(valueStartPosition);
                if (firstByte < MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                    return IntegerSize.LONG;
                } else if (firstByte > MOST_SIGNIFICANT_BYTE_OF_MIN_LONG) {
                    return IntegerSize.BIG_INTEGER;
                }
                for (int i = valueStartPosition + 1; i < valueEndPosition; i++) {
                    if (0x00 != buffer.peek(i)) {
                        return IntegerSize.BIG_INTEGER;
                    }
                }
            } else {
                // The largest positive 8-byte integer that can fit in a long is 0x7F_FF_FF_FF_FF_FF_FF_FF.
                if (buffer.peek(valueStartPosition) > MOST_SIGNIFICANT_BYTE_OF_MAX_LONG) {
                    return IntegerSize.BIG_INTEGER;
                }
            }
            return IntegerSize.LONG;
        }
        return IntegerSize.BIG_INTEGER;
    }

    /**
     * Require that the given type matches the type of the current value.
     * @param required the required type of current value.
     */
    private void requireType(IonType required) {
        if (required != valueType) {
            // Note: this is IllegalStateException to match the behavior of the other binary IonReader implementation.
            throw new IllegalStateException(
                String.format("Invalid type. Required %s but found %s.", required, valueType)
            );
        }
    }

    /**
     * Reads a VarUInt.
     * @return the value.
     */
    private int readVarUInt() {
        int currentByte = 0;
        int result = 0;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = buffer.peek(peekIndex++);
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result;
    }

    /**
     * Reads a UInt.
     * @param limit the position of the first byte after the end of the UInt value.
     * @return the value.
     */
    private long readUInt(int startIndex, int limit) {
        long result = 0;
        for (int i = startIndex; i < limit; i++) {
            result = (result << VALUE_BITS_PER_UINT_BYTE) | buffer.peek(i);
        }
        return result;
    }

    /**
     * Reads a UInt starting at `valueStartPosition` and ending at `valueEndPosition`.
     * @return the value.
     */
    private long readUInt() {
        return readUInt(valueStartPosition, valueEndPosition);
    }

    /**
     * Reads a VarInt.
     * @param firstByte the first byte of the VarInt representation, which has already been retrieved from the buffer.
     * @return the value.
     */
    private int readVarInt(int firstByte) {
        int currentByte = firstByte;
        int sign = (currentByte & VAR_INT_SIGN_BITMASK) == 0 ? 1 : -1;
        int result = currentByte & LOWER_SIX_BITS_BITMASK;
        while ((currentByte & HIGHEST_BIT_BITMASK) == 0) {
            currentByte = buffer.peek(peekIndex++);
            result = (result << VALUE_BITS_PER_VARUINT_BYTE) | (currentByte & LOWER_SEVEN_BITS_BITMASK);
        }
        return result * sign;
    }

    /**
     * Reads a VarInt.
     * @return the value.
     */
    private int readVarInt() {
        return readVarInt(buffer.peek(peekIndex++));
    }

    // Scratch space for various byte sizes. Only for use while computing a single value.
    private final byte[][] scratchForSize = new byte[][] {
        new byte[0],
        new byte[1],
        new byte[2],
        new byte[3],
        new byte[4],
        new byte[5],
        new byte[6],
        new byte[7],
        new byte[8],
        new byte[9],
        new byte[10],
        new byte[11],
        new byte[12],
    };

    /**
     * Copy the requested number of bytes from the buffer into a scratch buffer of exactly the requested length.
     * @param startIndex the start index from which to copy.
     * @param length the number of bytes to copy.
     * @return the scratch byte array.
     */
    private byte[] copyBytesToScratch(int startIndex, int length) {
        // Note: using reusable scratch buffers makes reading ints and decimals 1-5% faster and causes much less
        // GC churn.
        byte[] bytes = null;
        if (length < scratchForSize.length) {
            bytes = scratchForSize[length];
        }
        if (bytes == null) {
            bytes = new byte[length];
        }
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        buffer.copyBytes(startIndex, bytes, 0, bytes.length);
        return bytes;
    }

    /**
     * Reads a UInt value into a BigInteger.
     * @param isNegative true if the resulting BigInteger value should be negative; false if it should be positive.
     * @return the value.
     */
    private BigInteger readUIntAsBigInteger(boolean isNegative) {
        int length = valueEndPosition - valueStartPosition;
        // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
        // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
        // lead to a significant performance improvement.
        byte[] magnitude = copyBytesToScratch(valueStartPosition, length);
        int signum = isNegative ? -1 : 1;
        return new BigInteger(signum, magnitude);
    }

    /**
     * Get and clear the most significant bit in the given byte array.
     * @param intBytes bytes representing a signed int.
     * @return -1 if the most significant bit was set; otherwise, 1.
     */
    private int getAndClearSignBit(byte[] intBytes) {
        boolean isNegative = (intBytes[0] & HIGHEST_BIT_BITMASK) != 0;
        int signum = isNegative ? -1 : 1;
        if (isNegative) {
            intBytes[0] &= LOWER_SEVEN_BITS_BITMASK;
        }
        return signum;
    }

    /**
     * Reads an Int value into a BigInteger.
     * @param limit the position of the first byte after the end of the UInt value.
     * @return the value.
     */
    private BigInteger readIntAsBigInteger(int limit) {
        BigInteger value;
        int length = limit - peekIndex;
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor
            // until JDK 9, so copying to scratch space is always required. Migrating to the new constructor will
            // lead to a significant performance improvement.
            byte[] bytes = copyBytesToScratch(peekIndex, length);
            value = new BigInteger(getAndClearSignBit(bytes), bytes);
        }
        else {
            value = BigInteger.ZERO;
        }
        return value;
    }

    @Override
    public long longValue() {
        long value;
        if (valueType == IonType.INT) {
            if (valueTypeID.length == 0) {
                return 0;
            }
            value = readUInt();
            if (valueTypeID.isNegativeInt) {
                if (value == 0) {
                    throw new IonException("Int zero may not be negative.");
                }
                value *= -1;
            }
        } else if (valueType == IonType.FLOAT) {
            scalarConverter.addValue(doubleValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else if (valueType == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.long_value));
            value = scalarConverter.getLong();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("longValue() may only be called on values of type int, float, or decimal.");
        }
        return value;
    }

    @Override
    public BigInteger bigIntegerValue() {
        BigInteger value;
        if (valueType == IonType.INT) {
            if (isNullValue()) {
                // NOTE: this mimics existing behavior, but should probably be undefined (as, e.g., longValue() is in this
                //  case).
                return null;
            }
            if (valueTypeID.length == 0) {
                return BigInteger.ZERO;
            }
            value = readUIntAsBigInteger(valueTypeID.isNegativeInt);
            if (valueTypeID.isNegativeInt && value.signum() == 0) {
                throw new IonException("Int zero may not be negative.");
            }
        } else if (valueType == IonType.FLOAT) {
            if (isNullValue()) {
                value = null;
            } else {
                scalarConverter.addValue(doubleValue());
                scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.double_value);
                scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.bigInteger_value));
                value = scalarConverter.getBigInteger();
                scalarConverter.clear();
            }
        } else if (valueType == IonType.DECIMAL) {
            if (isNullValue()) {
                value = null;
            } else {
                scalarConverter.addValue(decimalValue());
                scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
                scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.bigInteger_value));
                value = scalarConverter.getBigInteger();
                scalarConverter.clear();
            }
        } else {
            throw new IllegalStateException("longValue() may only be called on values of type int, float, or decimal.");
        }
        return value;
    }

    @Override
    public Date dateValue() {
        Timestamp timestamp = timestampValue();
        if (timestamp == null) {
            return null;
        }
        return timestamp.dateValue();
    }

    @Override
    public int intValue() {
        return (int) longValue();
    }

    @Override
    public double doubleValue() {
        double value;
        if (valueType == IonType.FLOAT) {
            int length = valueEndPosition - valueStartPosition;
            if (length == 0) {
                return 0.0d;
            }
            ByteBuffer bytes = buffer.getByteBuffer(valueStartPosition, valueEndPosition);
            if (length == FLOAT_32_BYTE_LENGTH) {
                value = bytes.getFloat();
            } else {
                // Note: there is no need to check for other lengths here; the type ID byte is validated during next().
                value = bytes.getDouble();
            }
        }  else if (valueType == IonType.DECIMAL) {
            scalarConverter.addValue(decimalValue());
            scalarConverter.setAuthoritativeType(_Private_ScalarConversions.AS_TYPE.decimal_value);
            scalarConverter.cast(scalarConverter.get_conversion_fnid(_Private_ScalarConversions.AS_TYPE.double_value));
            value = scalarConverter.getDouble();
            scalarConverter.clear();
        } else {
            throw new IllegalStateException("doubleValue() may only be called on values of type float or decimal.");
        }
        return value;
    }

    /**
     * Decodes a string from the buffer into a String value.
     * @param valueStart the position in the buffer of the first byte in the string.
     * @param valueEnd the position in the buffer of the last byte in the string.
     * @return the value.
     */
    private String readString(int valueStart, int valueEnd) {
        ByteBuffer utf8InputBuffer = buffer.getByteBuffer(valueStart, valueEnd);
        int numberOfBytes = valueEnd - valueStart;
        return utf8Decoder.decode(utf8InputBuffer, numberOfBytes);
    }

    @Override
    public String stringValue() {
        String value;
        if (valueType == IonType.STRING) {
            if (isNullValue()) {
                return null;
            }
            value = readString(valueStartPosition, valueEndPosition);
        } else if (valueType == IonType.SYMBOL) {
            if (isNullValue()) {
                return null;
            }
            int sid = (int) readUInt();
            value = getSymbol(sid);
            if (value == null) {
                throw new UnknownSymbolException(sid);
            }
        } else {
            throw new IllegalStateException("Invalid type requested.");
        }
        return value;
    }

    @Override
    public SymbolToken symbolValue() {
        requireType(IonType.SYMBOL);
        if (isNullValue()) {
            return null;
        }
        int sid = (int) readUInt();
        return getSymbolToken(sid);
    }

    @Override
    public int byteSize() {
        if (!IonType.isLob(valueType) && !isNullValue()) {
            throw new IonException("Reader must be positioned on a blob or clob.");
        }
        return valueEndPosition - valueStartPosition;
    }

    @Override
    public byte[] newBytes() {
        byte[] bytes = new byte[byteSize()];
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        buffer.copyBytes(valueStartPosition, bytes, 0, bytes.length);
        return bytes;
    }

    @Override
    public int getBytes(byte[] bytes, int offset, int len) {
        int length = Math.min(len, byteSize() - lobBytesRead);
        // The correct number of bytes will be requested from the buffer, so the limit is set at the capacity to
        // avoid having to calculate a limit.
        buffer.copyBytes(valueStartPosition + lobBytesRead, bytes, offset, length);
        lobBytesRead += length;
        return length;
    }

    /**
     * Reads a decimal value as a BigDecimal.
     * @return the value.
     */
    private BigDecimal readBigDecimal() {
        int length = valueEndPosition - peekIndex;
        if (length == 0) {
            return BigDecimal.ZERO;
        }
        int scale = -readVarInt();
        BigDecimal value;
        if (length < LONG_SIZE_IN_BYTES) {
            // No need to allocate a BigInteger to hold the coefficient.
            long coefficient = 0;
            int sign = 1;
            if (peekIndex < valueEndPosition) {
                int firstByte = buffer.peek(peekIndex++);
                sign = (firstByte & HIGHEST_BIT_BITMASK) == 0 ? 1 : -1;
                coefficient = firstByte & LOWER_SEVEN_BITS_BITMASK;
            }
            while (peekIndex < valueEndPosition) {
                coefficient = (coefficient << VALUE_BITS_PER_UINT_BYTE) | buffer.peek(peekIndex++);
            }
            value = BigDecimal.valueOf(coefficient * sign, scale);
        } else {
            // The coefficient may overflow a long, so a BigInteger is required.
            value = new BigDecimal(readIntAsBigInteger(valueEndPosition), scale);
        }
        return value;
    }

    /**
     * Reads a decimal value as a Decimal.
     * @return the value.
     */
    private Decimal readDecimal() {
        int length = valueEndPosition - peekIndex;
        if (length == 0) {
            return Decimal.ZERO;
        }
        int scale = -readVarInt();
        BigInteger coefficient;
        length = valueEndPosition - peekIndex;
        if (length > 0) {
            // NOTE: unfortunately, there is no BigInteger(int signum, byte[] bits, int offset, int length) constructor,
            // so copying to scratch space is always required.
            byte[] bits = copyBytesToScratch(peekIndex, length);
            int signum = getAndClearSignBit(bits);
            // NOTE: there is a BigInteger.valueOf(long unscaledValue, int scale) factory method that avoids allocating
            // a BigInteger for coefficients that fit in a long. See its use in readBigDecimal() above. Unfortunately,
            // it is not possible to use this for Decimal because the necessary BigDecimal constructor is
            // package-private. If a compatible BigDecimal constructor is added in a future JDK revision, a
            // corresponding factory method should be added to Decimal to enable this optimization.
            coefficient = new BigInteger(signum, bits);
            if (coefficient.signum() == 0 && signum < 0) {
                return Decimal.negativeZero(scale);
            }
        }
        else {
            coefficient = BigInteger.ZERO;
        }
        return Decimal.valueOf(coefficient, scale);
    }

    @Override
    public BigDecimal bigDecimalValue() {
        requireType(IonType.DECIMAL);
        if (isNullValue()) {
            return null;
        }
        peekIndex = valueStartPosition;
        return readBigDecimal();
    }

    @Override
    public Decimal decimalValue() {
        requireType(IonType.DECIMAL);
        if (isNullValue()) {
            return null;
        }
        peekIndex = valueStartPosition;
        return readDecimal();
    }

    @Override
    public Timestamp timestampValue() {
        requireType(IonType.TIMESTAMP);
        if (isNullValue()) {
            return null;
        }
        peekIndex = valueStartPosition;
        int firstByte = buffer.peek(peekIndex++);
        Integer offset = null;
        if (firstByte != VAR_INT_NEGATIVE_ZERO) {
            offset = readVarInt(firstByte);
        }
        int year = readVarUInt();
        int month = 0;
        int day = 0;
        int hour = 0;
        int minute = 0;
        int second = 0;
        BigDecimal fractionalSecond = null;
        Timestamp.Precision precision = Timestamp.Precision.YEAR;
        if (peekIndex < valueEndPosition) {
            month = readVarUInt();
            precision = Timestamp.Precision.MONTH;
            if (peekIndex < valueEndPosition) {
                day = readVarUInt();
                precision = Timestamp.Precision.DAY;
                if (peekIndex < valueEndPosition) {
                    hour = readVarUInt();
                    if (peekIndex >= valueEndPosition) {
                        throw new IonException("Timestamps may not specify hour without specifying minute.");
                    }
                    minute = readVarUInt();
                    precision = Timestamp.Precision.MINUTE;
                    if (peekIndex < valueEndPosition) {
                        second = readVarUInt();
                        precision = Timestamp.Precision.SECOND;
                        if (peekIndex < valueEndPosition) {
                            fractionalSecond = readBigDecimal();
                            if (fractionalSecond.signum() < 0 || fractionalSecond.compareTo(BigDecimal.ONE) >= 0) {
                                throw new IonException("The fractional seconds value in a timestamp must be greater" +
                                        "than or equal to zero and less than one.");
                            }
                        }
                    }
                }
            }
        }
        try {
            return Timestamp.createFromUtcFields(
                    precision,
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    fractionalSecond,
                    offset
            );
        } catch (IllegalArgumentException e) {
            throw new IonException("Illegal timestamp encoding. ", e);
        }
    }

    /**
     * Gets the annotation symbol IDs for the current value, reading them from the buffer first if necessary.
     * @return the annotation symbol IDs, or an empty list if the current value is not annotated.
     */
    private IntList getAnnotationSids() {
        if (annotationSids.isEmpty()) {
            int savedPeekIndex = peekIndex;
            peekIndex = annotationStartPosition;
            while (peekIndex < annotationEndPosition) {
                annotationSids.add(readVarUInt());
            }
            peekIndex = savedPeekIndex;
        }
        return annotationSids;
    }

    @Override
    public String[] getTypeAnnotations() {
        if (hasAnnotations) {
            IntList annotationSids = getAnnotationSids();
            String[] annotationArray = new String[annotationSids.size()];
            for (int i = 0; i < annotationArray.length; i++) {
                String symbol = getSymbol(annotationSids.get(i));
                if (symbol == null) {
                    throw new UnknownSymbolException(annotationSids.get(i));
                }
                annotationArray[i] = symbol;
            }
            return annotationArray;
        }
        return _Private_Utils.EMPTY_STRING_ARRAY;
    }

    @Override
    public SymbolToken[] getTypeAnnotationSymbols() {
        if (hasAnnotations) {
            IntList annotationSids = getAnnotationSids();
            SymbolToken[] annotationArray = new SymbolToken[annotationSids.size()];
            for (int i = 0; i < annotationArray.length; i++) {
                annotationArray[i] = getSymbolToken(annotationSids.get(i));
            }
            return annotationArray;
        }
        return SymbolToken.EMPTY_ARRAY;
    }

    private static final Iterator<String> EMPTY_ITERATOR = new Iterator<String>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove from an empty iterator.");
        }
    };

    @Override
    public Iterator<String> iterateTypeAnnotations() {
        if (hasAnnotations) {
            if (isAnnotationIteratorReuseEnabled) {
                annotationIterator.ready();
                return annotationIterator;
            } else {
                return new SingleUseAnnotationIterator();
            }
        }
        return EMPTY_ITERATOR;
    }

    @Override
    public int getFieldId() {
        return fieldNameSid;
    }

    @Override
    public String getFieldName() {
        if (fieldNameSid < 0) {
            return null;
        }
        String fieldName = getSymbol(fieldNameSid);
        if (fieldName == null) {
            throw new UnknownSymbolException(fieldNameSid);
        }
        return fieldName;
    }

    @Override
    public SymbolToken getFieldNameSymbol() {
        if (fieldNameSid < 0) {
            return null;
        }
        return getSymbolToken(fieldNameSid);
    }

    @Override
    public boolean isNullValue() {
        return valueTypeID != null && valueTypeID.isNull;
    }

    @Override
    public boolean isInStruct() {
        return !containerStack.isEmpty() && containerStack.peek().type == IonType.STRUCT;
    }

    @Override
    public boolean booleanValue() {
        requireType(IonType.BOOL);
        return valueTypeID.lowerNibble == 1;
    }

    @Override
    public <T> T asFacet(Class<T> facetType) {
        return null;
    }

    @Override
    public void requireCompleteValue() {
        // NOTE: If we want to replace the other binary IonReader implementation with this one, the following
        // validation could be performed in next() if incremental mode is not enabled. That would allow this
        // implementation to behave in the same way as the other implementation when an incomplete value is
        // encountered.
        if (lookahead.isSkippingCurrentValue()) {
            throw new IonException("Unexpected EOF.");
        }
        if (lookahead.available() > 0 && lookahead.moreDataRequired()) {
            if (lookahead.getIvmIndex() < 0
                || lookahead.available() != _Private_IonConstants.BINARY_VERSION_MARKER_SIZE) {
                throw new IonException("Unexpected EOF.");
            }
        }
    }

    @Override
    public void close() throws IOException {
        requireCompleteValue();
        inputStream.close();
        utf8Decoder.close();
    }

}
