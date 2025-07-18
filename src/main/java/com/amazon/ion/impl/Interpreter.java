package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.ReadOnlyValueException;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.SystemSymbols;
import com.amazon.ion.Timestamp;
import com.amazon.ion.UnknownSymbolException;
import com.amazon.ion.ValueFactory;
import com.amazon.ion.impl.bin.IntList;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroCompiler;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.MacroTable;
import com.amazon.ion.impl.macro.MutableMacroTable;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterContinuable;
import com.amazon.ion.impl.macro.SystemMacro;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.system.SimpleCatalog;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import static com.amazon.ion.SystemSymbols.DEFAULT_MODULE;
import static com.amazon.ion.SystemSymbols.IMPORTS_SID;
import static com.amazon.ion.SystemSymbols.ION;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE_SID;
import static com.amazon.ion.SystemSymbols.MAX_ID_SID;
import static com.amazon.ion.SystemSymbols.NAME_SID;
import static com.amazon.ion.SystemSymbols.SYMBOLS_SID;
import static com.amazon.ion.SystemSymbols.VERSION_SID;
import static com.amazon.ion.impl.SharedSymbolTable.getSystemSymbolTable;
import static com.amazon.ion.impl._Private_Utils.EMPTY_STRING_ARRAY;

public class Interpreter extends DelegatingIonReaderContinuableApplication {

    // TODO system-level Ion 1.0 transcoding
    // TODO verbatim macro transcoding (MacroAwareIonReader)

    private static final int OPERATION_RIGHT_SHIFT = 24;
    private static final int ONE_BYTE_MASK = 0xFF;
    private static final int DATA_MASK = 0x00FFFFFF;
    private static final IonType[] ION_TYPES_BY_ORDINAL = IonType.values();

    private static final byte[] IMPORTS_UTF8 = SystemSymbols.IMPORTS.getBytes(StandardCharsets.UTF_8);
    private static final byte[] SYMBOLS_UTF8 = SystemSymbols.SYMBOLS.getBytes(StandardCharsets.UTF_8);
    private static final byte[] NAME_UTF8 = SystemSymbols.NAME.getBytes(StandardCharsets.UTF_8);
    private static final byte[] VERSION_UTF8 = SystemSymbols.VERSION.getBytes(StandardCharsets.UTF_8);
    private static final byte[] MAX_ID_UTF8 = SystemSymbols.MAX_ID.getBytes(StandardCharsets.UTF_8);

    // An IonCatalog containing zero shared symbol tables.
    private static final IonCatalog EMPTY_CATALOG = new SimpleCatalog();

    // Initial capacity of the ArrayList used to hold the text in the current symbol table.
    static final int SYMBOLS_LIST_INITIAL_CAPACITY = 128;

    // The imports for Ion 1.0 data with no shared user imports.
    private static final LocalSymbolTableImports ION_1_0_IMPORTS
        = new LocalSymbolTableImports(getSystemSymbolTable(1));

    // The catalog used by the reader to resolve shared symbol table imports.
    private final IonCatalog catalog;

    // The shared symbol tables imported by the local symbol table that is currently in scope.
    private LocalSymbolTableImports imports = ION_1_0_IMPORTS;

    // The first (lowest) local symbol ID in the symbol table.
    private int firstLocalSymbolId = imports.getMaxId() + 1;

    // The cached SymbolTable representation of the current local symbol table. Invalidated whenever a local
    // symbol table is encountered in the stream.
    private SymbolTable cachedReadOnlySymbolTable = null;

    private StackFrame[] stack = new StackFrame[8];
    private int stackFrameIndex = -1;
    private StackFrame top = null;
    private EncodingContext encodingContext = EncodingContext.getDefault();

    // The text representations of the symbol table that is currently in scope, indexed by symbol ID. If the element at
    // a particular index is null, that symbol has unknown text.
    protected String[] symbols = new String[SYMBOLS_LIST_INITIAL_CAPACITY];

    // The maximum offset into the 'symbols' array that points to a valid local symbol.
    protected int localSymbolMaxOffset = -1;

    // The maximum offset into the macro table that points to a valid local macro.
    private int localMacroMaxOffset = -1;

    private boolean yield = true;

    final ParsingIonCursorBinary rawCursor;

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    // TODO everywhere in this class, avoid new*, use pooling

    Interpreter(IonReaderBuilder builder, InputStream inputStream, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen, ParsingIonCursorBinary.PrepareScalarFunction prepareScalarFunction) {
        // TODO accept an option that allows the user to specify whether to push this core-level reader first, or a top/application level reader
        rawCursor = new ParsingIonCursorBinary(builder.getBufferConfiguration(), inputStream, alreadyRead, alreadyReadOff, alreadyReadLen, prepareScalarFunction);
        rawCursor.setEncodingContextSupplier(this::getEncodingContext); // TODO this can go into the constructor
        SymbolResolvingIonCursor symbolResolvingCursor = new SymbolResolvingIonCursor().initialize(rawCursor);
        IonReaderContinuableApplication coreCursor = new MacroAwareIonCursorBinary().initialize(symbolResolvingCursor);
        // TODO this creates too many layers of delegation, which may impact performance. Look into combining functionality of the cursor types that are independent of the stack.
        //  Probably via inheritance...
        pushStackFrame().initializeDataSource(new ApplicationLevelIonCursor().initialize(new EncodingDirectiveAwareIonCursor().initialize(coreCursor), coreCursor));
        catalog = builder.getCatalog() == null ?  EMPTY_CATALOG : builder.getCatalog();
        // TODO check the following. Should it go in ApplicationLevelIonCursor?
        resetImports(getIonMajorVersion(), getIonMinorVersion());
        registerIvmNotificationConsumer((x, y) -> resetEncodingContext());
    }

    Interpreter(IonReaderBuilder builder, byte[] data, int offset, int length, ParsingIonCursorBinary.PrepareScalarFunction prepareScalarFunction) {
        // TODO accept an option that allows the user to specify whether to push this core-level reader first, or a top/application level reader
        rawCursor = new ParsingIonCursorBinary(builder.getBufferConfiguration(), data, offset, length, prepareScalarFunction);
        rawCursor.setEncodingContextSupplier(this::getEncodingContext);
        SymbolResolvingIonCursor symbolResolvingCursor = new SymbolResolvingIonCursor().initialize(rawCursor);
        IonReaderContinuableApplication coreCursor = new MacroAwareIonCursorBinary().initialize(symbolResolvingCursor);
        pushStackFrame().initializeDataSource(new ApplicationLevelIonCursor().initialize(new EncodingDirectiveAwareIonCursor().initialize(coreCursor), coreCursor));
        catalog = builder.getCatalog() == null ?  EMPTY_CATALOG : builder.getCatalog();
        // TODO check the following. Should it go in ApplicationLevelIonCursor?
        resetImports(getIonMajorVersion(), getIonMinorVersion());
        registerIvmNotificationConsumer((x, y) -> resetEncodingContext());
        // TODO ensure this is covered by all the existing tests.
        rawCursor.registerOversizedValueHandler(
            () -> {
                boolean mightBeSymbolTable = true;
                if (!(top.dataSource instanceof SymbolTableInterceptingIonCursor)) {
                    // The reader is not currently processing a symbol table.
                    if (getDepth() > 0 || !hasAnnotations()) {
                        // Only top-level annotated values can be symbol tables.
                        mightBeSymbolTable = false;
                    } else if (top.dataSource instanceof ApplicationLevelIonCursor) { // TODO check //if (annotationSequenceMarker.startIndex >= 0 && annotationSequenceMarker.endIndex <= limit) {
                        // The annotations on the value are available.
                        if (((ApplicationLevelIonCursor) top.dataSource).startsWithIonSymbolTable()) {
                            // The first annotation on the value is $ion_symbol_table. It may be a symbol table if
                            // its type is not yet known (null); it is definitely a symbol table if its type is STRUCT.
                            IonType type = super.getType();
                            mightBeSymbolTable = type == null || type == IonType.STRUCT;
                        } else {
                            // The first annotation on the value is not $ion_symbol_table, so it cannot be a symbol table.
                            mightBeSymbolTable = false;
                        }
                    }
                }
                if (mightBeSymbolTable) {
                    builder.getBufferConfiguration().getOversizedSymbolTableHandler().onOversizedSymbolTable();
                    rawCursor.terminate();
                } else {
                    builder.getBufferConfiguration().getOversizedValueHandler().onOversizedValue();
                }
            }
        );
    }

    /*
    public void setEncodingContext(EncodingContext encodingContext) {
        this.encodingContext = encodingContext;
    }

     */

    private class StackFrame {

        private ArgumentSource argumentSource;
        private IonReaderContinuableApplication dataSource;
        private boolean yieldByDefault = true;

        private BytecodeCursor reusableBytecodeCursor = new BytecodeCursor();
        private LazyArgumentSource reusableLazyArgumentSource = new LazyArgumentSource();

        StackFrame initializeBytecodeDataSource(Bytecode bytecode, int programCounterStart, int programCounterLimit, boolean popStackWhenExhausted, ArgumentSource parentArguments, String fieldName) {
            reusableBytecodeCursor.initialize(bytecode, programCounterStart, programCounterLimit, popStackWhenExhausted, parentArguments, fieldName);
            dataSource = reusableBytecodeCursor;
            setDelegate(dataSource);
            return this;
        }

        StackFrame initializeDataSource(IonReaderContinuableApplication source) {
            dataSource = source;
            setDelegate(dataSource);
            return this;
        }

        void initializeLazyArgumentSource(PresenceBitmap presenceBitmap, SymbolResolvingIonCursor dataSource, List<Macro.Parameter> signature) {
            reusableLazyArgumentSource.initialize(presenceBitmap, dataSource, signature);
            argumentSource = reusableLazyArgumentSource;
        }

        void initializeArgumentSource(ArgumentSource source) {
            argumentSource = source;
        }
    }

    private final IonReaderContinuableApplication voidCursor = new DelegatingIonReaderContinuableApplication() {

        @Override
        public Event nextValue() {
            popStackFrame();
            return Event.NEEDS_INSTRUCTION;
        }

        @Override
        public Event stepIntoContainer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event stepOutOfContainer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event fillValue() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Event getCurrentEvent() {
            return Event.NEEDS_INSTRUCTION;
        }

        @Override
        public Event endStream() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            // Do nothing.
        }
    };

    private class ArgumentReference {

        private int startIndex;
        private int endIndex;
        private Bytecode bytecode;
        private ArgumentSource source;
        private ArgumentSource parentArguments;

        private final BytecodeCursor reusableBytecodeCursor = new BytecodeCursor();

        ArgumentReference initialize(Bytecode bytecode, int startIndex, int endIndex, ArgumentSource parentArguments) {
            this.bytecode = bytecode;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.parentArguments = parentArguments;
            source = null;
            return this;
        }

        ArgumentReference initialize(ArgumentSource source, int sourceIndex) {
            this.source = source;
            this.startIndex = sourceIndex;
            this.endIndex = -1;
            this.bytecode = null;
            return this;
        }

        public IonReaderContinuableApplication evaluate(String fieldName) {
            if (source == null) {
                return reusableBytecodeCursor.initialize(bytecode, startIndex, endIndex, false, parentArguments, fieldName);
            }
            return source.evaluateArgument(startIndex, fieldName);
        }
    }

    private interface ArgumentSource {
        IonReaderContinuableApplication evaluateArgument(int argumentIndex, String fieldName);
        void close();
    }

    private class BytecodeArgumentSource implements ArgumentSource {
        private Bytecode bytecode;
        private ArgumentReference[] argumentRegisters = new ArgumentReference[8];
        private int numberOfArguments = 0;
        //private ArgumentSource parentArguments;

        BytecodeArgumentSource initialize(Bytecode bytecode/*, ArgumentSource parentArguments*/) {
            this.bytecode = bytecode;
            //this.parentArguments = parentArguments;
            numberOfArguments = 0;
            return this;
        }

        boolean isNotInitialized() {
            return bytecode == null;
        }

        private ArgumentReference addArgument() {
            if (numberOfArguments >= argumentRegisters.length) {
                ArgumentReference[] newArgumentRegisters = new ArgumentReference[argumentRegisters.length * 2];
                System.arraycopy(argumentRegisters, 0, newArgumentRegisters, 0, argumentRegisters.length);
                argumentRegisters = newArgumentRegisters;
            }
            ArgumentReference argumentRegister = argumentRegisters[numberOfArguments++];
            if (argumentRegister == null) {
                argumentRegister = new ArgumentReference();
                argumentRegisters[numberOfArguments - 1] = argumentRegister;
            }
            return argumentRegister;
        }

        void addArgument(int startIndex, int endIndex, ArgumentSource parentArguments) {
            addArgument().initialize(bytecode, startIndex, endIndex, parentArguments);
        }

        /*
        void addArgument(ArgumentSource source, int sourceIndex) {
            addArgument().initialize(source, sourceIndex);
        }

         */

        @Override
        public IonReaderContinuableApplication evaluateArgument(int argumentIndex, String fieldName) {
            return argumentRegisters[argumentIndex].evaluate(fieldName);
        }

        @Override
        public void close() {
            // Nothing to do
        }
    }

    private class LazyArgumentSource implements ArgumentSource {

        private PresenceBitmap presenceBitmap;
        private SymbolResolvingIonCursor dataSource;
        private List<Macro.Parameter> signature;
        private int firstArgumentStart;
        private int firstArgumentEnd;
        private IonTypeID firstArgumentTypeId;
        private int currentArgumentIndex;

        private int numberOfArgumentsRequested;
        private LimitedIonCursor[] argumentRegisters = new LimitedIonCursor[8];

        LazyArgumentSource initialize(PresenceBitmap presenceBitmap, SymbolResolvingIonCursor dataSource, List<Macro.Parameter> signature) {
            this.presenceBitmap = presenceBitmap;
            this.dataSource = dataSource;
            this.signature = signature;
            // TODO: position the cursor on argument 0? Or not necessary?
            //dataSource.nextValue(); // TODO handling of the event?
            firstArgumentStart = (int) dataSource.cursor.valueMarker.startIndex;
            firstArgumentEnd = (int) dataSource.cursor.valueMarker.endIndex;
            firstArgumentTypeId = dataSource.cursor.valueMarker.typeId;
            currentArgumentIndex = -1;
            numberOfArgumentsRequested = 0;
            return this;
        }

        private LimitedIonCursor addArgument() {
            if (numberOfArgumentsRequested >= argumentRegisters.length) {
                LimitedIonCursor[] newArgumentRegisters = new LimitedIonCursor[argumentRegisters.length * 2];
                System.arraycopy(argumentRegisters, 0, newArgumentRegisters, 0, argumentRegisters.length);
                argumentRegisters = newArgumentRegisters;
            }
            LimitedIonCursor argumentRegister = argumentRegisters[numberOfArgumentsRequested++];
            if (argumentRegister == null) {
                argumentRegister = new LimitedIonCursor();
                argumentRegisters[numberOfArgumentsRequested - 1] = argumentRegister;
            }
            return argumentRegister;
        }

        @Override
        public IonReaderContinuableApplication evaluateArgument(int argumentIndex, String fieldName) {
            long presence = presenceBitmap.get(argumentIndex);
            if (presence == PresenceBitmap.VOID) {
                return voidCursor;
            }
            // Position the reader on the argument with the given index
            if (argumentIndex != currentArgumentIndex) {
                if (argumentIndex == currentArgumentIndex + 1) {
                    currentArgumentIndex += 1;
                } else {
                    dataSource.cursor.sliceAfterHeader(firstArgumentStart, firstArgumentEnd, firstArgumentTypeId);
                    for (int i = 0; i < argumentIndex - 1; i++) {
                        // Position the cursor just before the argument to be read. The limited cursor will call
                        // nextValue() to position the cursor on the desired argument.
                        // TODO different flavors of next depending on argument encoding
                        dataSource.nextValue(); // TODO handling of the event?
                    }
                    currentArgumentIndex = argumentIndex + 1;
                }
            }

            if (presence == PresenceBitmap.EXPRESSION) {
                return addArgument().initialize(dataSource, signature.get(argumentIndex), false, fieldName);
            } else if (presence == PresenceBitmap.GROUP) {
                return addArgument().initialize(dataSource, signature.get(argumentIndex), true, fieldName);
            } else {
                throw new IonException("Illegal presence bits.");
            }
        }

        @Override
        public void close() {
            dataSource.cursor.stepOutOfEExpressionFrom(currentArgumentIndex + 1, signature, presenceBitmap);
        }
    }

    private class LimitedIonCursor extends DelegatingIonReaderContinuableApplication {

        private IonCursorBinary cursor;
        private int startDepth;
        private boolean isExhausted;
        private boolean isExpressionGroup;
        private Macro.Parameter parameter;
        private String fieldName = null;

        LimitedIonCursor initialize(SymbolResolvingIonCursor delegate, Macro.Parameter parameter, boolean isExpressionGroup, String fieldName) {
            this.cursor = delegate.cursor;
            setDelegate(delegate);
            startDepth = cursor.containerIndex;
            isExhausted = false;
            this.parameter = parameter;
            this.isExpressionGroup = isExpressionGroup;
            this.fieldName = fieldName;
            if (isExpressionGroup) {
                if (parameter.getType() == Macro.ParameterEncoding.Tagged) {
                    cursor.enterTaggedArgumentGroup();
                } else {
                    cursor.enterTaglessArgumentGroup(parameter.getType().taglessEncodingKind);
                }
            }
            return this;
        }


        @Override
        public Event nextValue() {
            if (isExhausted) {
                if (isExpressionGroup) {
                    cursor.exitArgumentGroup();
                }
                popStackFrame();
                return Event.NEEDS_INSTRUCTION;
            }
            Event event;
            if (isExpressionGroup) {
                event = cursor.nextGroupedValue();
            } else {
                if (parameter.getType() == Macro.ParameterEncoding.Tagged) {
                    event = cursor.nextValue();
                } else {
                    event = cursor.nextTaglessValue(parameter.getType().taglessEncodingKind);
                }
            }
            if (event == Event.START_SCALAR) {
                if (cursor.containerIndex == startDepth) {
                    isExhausted = true;
                }
            }
            if (event == Event.NEEDS_INSTRUCTION) {
                if (cursor.valueTid != null && cursor.valueTid.isMacroInvocation) {
                    // TODO the delegate handles the evaluation... what is there to do here if anything?
                    if (cursor.containerIndex == startDepth) {
                        isExhausted = true;
                    }
                }
            }
            return event;
        }

        @Override
        public Event stepOutOfContainer() {
            Event event = cursor.stepOutOfContainer();
            // TODO this returns NEEDS_INSTRUCTION, which clashes with the exhausted condition. Needs fix
            if (cursor.containerIndex == startDepth) {
                isExhausted = true;
            }
            return event;
        }

        @Override
        public boolean hasFieldText() {
            return fieldName != null;
        }

        @Override
        public String getFieldText() {
            return fieldName;
        }

        @Override
        public SymbolToken getFieldNameSymbol() {
            return _Private_Utils.newSymbolToken(fieldName);
        }

        @Override
        public void close() throws IOException {
            // Do nothing.
        }
    }

    /**
     * Determines whether the bytes between [start, end) in 'buffer' match the target bytes.
     * @param target the target bytes.
     * @param buffer the bytes to match.
     * @param start index of the first byte to match.
     * @param end index of the first byte after the last byte to match.
     * @return true if the bytes match; otherwise, false.
     */
    static boolean bytesMatch(byte[] target, byte[] buffer, int start, int end) {
        // TODO if this ends up on a critical performance path, see if it's faster to copy the bytes into a
        //  pre-allocated buffer and then perform a comparison. It's possible that a combination of System.arraycopy()
        //  and Arrays.equals(byte[], byte[]) is faster because it can be more easily optimized with native code by the
        //  JVM—both are annotated with @HotSpotIntrinsicCandidate.
        int length = end - start;
        if (length != target.length) {
            return false;
        }
        for (int i = 0; i < target.length; i++) {
            if (target[i] != buffer[start + i]) {
                return false;
            }
        }
        return true;
    }

    private class EncodingDirectiveAwareIonCursor extends DelegatingIonReaderContinuableApplication {

        private final EncodingDirectiveInterceptingIonCursor encodingDirectiveInterceptingCursor = new EncodingDirectiveInterceptingIonCursor();

        EncodingDirectiveAwareIonCursor initialize(IonReaderContinuableApplication delegate) {
            setDelegate(delegate);
            return this;
        }

        /**
         * @return true if current value has a sequence of annotations that begins with `$ion`; otherwise, false.
         */
        boolean startsWithIonAnnotation() {
            if (rawCursor.minorVersion > 0) {
                Marker marker = rawCursor.annotationTokenMarkers.get(0);
                return matchesSystemSymbol_1_1(marker, SystemSymbols_1_1.ION);
            }
            return false;
        }

        @Override
        public String getSymbol(int sid) {
            // Only symbol IDs declared in Ion 1.1 encoding directives (not Ion 1.0 symbol tables) are resolved by the
            // core reader. In Ion 1.0, 'symbols' is never populated by the core reader.
            if (sid > 0 && sid - 1 <= localSymbolMaxOffset) {
                return symbols[sid - 1];
            }
            return null;
        }

        /**
         * Returns true if the symbol at `marker`...
         * <p> * is a system symbol with the same ID as the expected System Symbol
         * <p> * is an inline symbol with the same utf8 bytes as the expected System Symbol
         * <p> * is a user symbol that maps to the same text as the expected System Symbol
         * <p>
         */
        boolean matchesSystemSymbol_1_1(Marker marker, SystemSymbols_1_1 systemSymbol) {
            if (marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                return systemSymbol.getText().equals(rawCursor.getSystemSymbolToken(marker).getText());
            } else if (marker.startIndex < 0) {
                // This is a local symbol whose ID is stored in marker.endIndex.
                return systemSymbol.getText().equals(getSymbol((int) marker.endIndex));
            } else {
                // This is an inline symbol with UTF-8 bytes bounded by the marker.
                return bytesMatch(systemSymbol.getUtf8Bytes(), rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex);
            }
        }

        /**
         * @return true if the reader is positioned on an encoding directive; otherwise, false.
         */
        private boolean isPositionedOnEncodingDirective() {
            return rawCursor.event == Event.START_CONTAINER
                && rawCursor.hasAnnotations
                && delegate.getType() == IonType.SEXP //valueTid.type == IonType.SEXP
                //&& parent == null
                && startsWithIonAnnotation();
        }

        /**
         * @return true if the macro evaluator is positioned on an encoding directive; otherwise, false.
         */
        /*
        private boolean isPositionedOnEvaluatedEncodingDirective() { // TODO this is probably needed, or some better abstraction for checking encoding directive and symbol table.
            if (macroEvaluatorIonReader.getType() != IonType.SEXP) {
                return false;
            }
            Iterator<String> annotations = macroEvaluatorIonReader.iterateTypeAnnotations();
            return annotations.hasNext()
                && annotations.next().equals(SystemSymbols_1_1.ION.getText());
        }

         */

        @Override
        public Event nextValue() {
            Event event = delegate.nextValue();
            if (isPositionedOnEncodingDirective()) {
                pushStackFrame().initializeDataSource(encodingDirectiveInterceptingCursor.initialize(delegate)); // TODO argument source?
                top.yieldByDefault = false;
                event = Event.NEEDS_INSTRUCTION;
            }
            return event;
        }
    }

    /**
     * The reader's state. `READING_VALUE` indicates that the reader is reading a raw value; all other states
     * indicate that the reader is in the middle of reading an encoding directive.
     */
    private enum EncodingDirectiveState {
        ON_DIRECTIVE_SEXP,
        IN_DIRECTIVE_SEXP,
        IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME,
        IN_MODULE_DIRECTIVE_SEXP_BODY,
        ON_SEXP_IN_MODULE_DIRECTIVE,
        IN_SEXP_IN_MODULE_DIRECTIVE,
        CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE,
        IN_SYMBOL_TABLE_SEXP,
        IN_APPENDED_SYMBOL_TABLE,
        ON_SYMBOL_TABLE_LIST,
        IN_SYMBOL_TABLE_LIST,
        ON_SYMBOL,
        IN_MACRO_TABLE_SEXP,
        IN_APPENDED_MACRO_TABLE,
        ON_MACRO_SEXP,
        COMPILING_MACRO,
        READING_VALUE, // TODO still necessary/
    }

    private class EncodingDirectiveInterceptingIonCursor extends DelegatingIonReaderContinuableApplication {
        boolean isSymbolTableAppend = false;
        boolean isMacroTableAppend = false;
        List<String> newSymbols = new ArrayList<>(128);
        Map<MacroRef, Macro> newMacros = new LinkedHashMap<>();

        MacroCompiler macroCompiler;

        boolean isSymbolTableAlreadyClassified = false;
        boolean isMacroTableAlreadyClassified = false;

        // The current state.
        private EncodingDirectiveState state = EncodingDirectiveState.READING_VALUE;

        EncodingDirectiveInterceptingIonCursor initialize(IonReaderContinuableApplication delegate) {
            setDelegate(delegate);
            // TODO enable reuse of the adapter and compiler
            ReaderAdapter readerAdapter = new ReaderAdapterContinuable(delegate);
            macroCompiler = new MacroCompiler(this::resolveMacro, readerAdapter);
            isSymbolTableAppend = false;
            isSymbolTableAlreadyClassified = false;
            newSymbols.clear();
            isMacroTableAppend = false;
            isMacroTableAlreadyClassified = false;
            newMacros.clear();
            state = EncodingDirectiveState.ON_DIRECTIVE_SEXP;
            return this;
        }

        private Macro resolveMacro(MacroRef macroRef) {
            Macro newMacro = newMacros.get(macroRef);
            if (newMacro == null) {
                newMacro = encodingContext.getMacroTable().get(macroRef);
            }
            return newMacro;
        }

        private boolean valueUnavailable() {
            /* TODO verify this is handled properly by the reader stack
            if (isEvaluatingEExpression) {
                return false;
            }

             */
            Event event = fillValue();
            return event == Event.NEEDS_DATA || event == Event.NEEDS_INSTRUCTION;
        }

        private void classifyDirective() {
            errorIf(getEncodingType() != IonType.SYMBOL, "Ion encoding directives must start with a directive keyword.");
            String name = getSymbolText();
            // TODO: Add support for `import` and `encoding` directives
            if (SystemSymbols_1_1.MODULE.getText().equals(name)) {
                state = EncodingDirectiveState.IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME;
            } else if (SystemSymbols_1_1.IMPORT.getText().equals(name)) {
                throw new IonException("'import' directive not yet supported");
            } else if (SystemSymbols_1_1.ENCODING.getText().equals(name)) {
                throw new IonException("'encoding' directive not yet supported");
            } else {
                throw new IonException(String.format("'%s' is not a valid directive keyword", name));
            }
        }

        private void classifySexpWithinModuleDirective() {
            String name = getSymbolText();
            if (SystemSymbols_1_1.SYMBOL_TABLE.getText().equals(name)) {
                state = EncodingDirectiveState.IN_SYMBOL_TABLE_SEXP;
            } else if (SystemSymbols_1_1.MACRO_TABLE.getText().equals(name)) {
                state = EncodingDirectiveState.IN_MACRO_TABLE_SEXP;
            } else {
                // TODO: add support for 'module' and 'import' clauses
                throw new IonException(String.format("'%s' clause not supported in module definition", name));
            }
        }

        /**
         * Classifies a symbol table as either 'set' or 'append'. The caller must ensure the reader is positioned within
         * a symbol table (after the symbol 'symbol_table') before calling. Upon return, the reader will be positioned
         * on a list in the symbol table.
         */
        private void classifySymbolTable() {
            IonType type = getEncodingType();
            if (isSymbolTableAlreadyClassified) {
                if (type != IonType.LIST) { // TODO support module name imports
                    throw new IonException("symbol_table s-expression must contain list(s) of symbols.");
                }
                state = EncodingDirectiveState.ON_SYMBOL_TABLE_LIST;
                return;
            }
            isSymbolTableAlreadyClassified = true;
            if (IonType.isText(type)) {
                if (DEFAULT_MODULE.equals(stringValue()) && !isSymbolTableAppend) {
                    state = EncodingDirectiveState.IN_APPENDED_SYMBOL_TABLE;
                } else {
                    throw new IonException("symbol_table s-expression must begin with either '_' or a list.");
                }
            } else if (type == IonType.LIST) {
                state = EncodingDirectiveState.ON_SYMBOL_TABLE_LIST;
            } else {
                throw new IonException("symbol_table s-expression must begin with either '_' or a list.");
            }
        }

        /**
         * Classifies a macro table as either 'set' or 'append'. The caller must ensure the reader is positioned within
         * a macro table (after the symbol 'macro_table') before calling. Upon return, the reader will be positioned
         * on an s-expression in the macro table.
         */
        private void classifyMacroTable() {
            IonType type = getEncodingType();
            if (isMacroTableAlreadyClassified) {
                if (type != IonType.SEXP) {
                    throw new IonException("macro_table s-expression must contain s-expression(s).");
                }
                state = EncodingDirectiveState.ON_MACRO_SEXP;
                return;
            }
            isMacroTableAlreadyClassified = true;
            if (IonType.isText(type)) {
                if (DEFAULT_MODULE.equals(stringValue()) && !isMacroTableAppend) {
                    state = EncodingDirectiveState.IN_APPENDED_MACRO_TABLE;
                } else {
                    throw new IonException("macro_table s-expression must begin with either '_' or s-expression(s).");
                }
            } else if (type == IonType.SEXP) {
                localMacroMaxOffset = -1;
                state = EncodingDirectiveState.ON_MACRO_SEXP;
            } else {
                throw new IonException("macro_table s-expression must contain s-expression(s).");
            }
        }

        private void stepOutOfSexpWithinEncodingDirective() {
            stepOutOfContainer();
            state = EncodingDirectiveState.IN_MODULE_DIRECTIVE_SEXP_BODY;
        }

        /**
         * Install `newMacros`, initializing a macro evaluator capable of evaluating them.
         */
        private void installMacros() {
            if (!isMacroTableAppend) {
                encodingContext = new EncodingContext(new MutableMacroTable(MacroTable.empty()));
                // TODO convey this to the e-expression args reader so it can reset its invocation cache.
            } else if (!encodingContext.isMutable()) { // we need to append, but can't
                encodingContext = new EncodingContext(new MutableMacroTable(encodingContext.getMacroTable()));
            }

            if (newMacros.isEmpty()) return; // our work is done

            encodingContext.getMacroTable().putAll(newMacros);
        }

        /**
         * Install any new symbols and macros, step out of the encoding directive, and resume reading raw values.
         */
        private void finishEncodingDirective() {
            if (!isSymbolTableAppend) {
                resetSymbolTable();
            }
            installSymbols(newSymbols);
            installMacros();
            stepOutOfContainer();
            state = EncodingDirectiveState.READING_VALUE;
        }

        /**
         * Navigate to the next value at the core level (without interpretation by subclasses).
         * @return the event that conveys the result of the operation.
         */
        private Event coreNextValue() {
            /*
            if (isEvaluatingEExpression) {
                evaluateNext();
                return event;
            } else {
                return IonReaderContinuableCoreBinary.super.nextValue();
            }

             */
            return delegate.nextValue(); // TODO verify the reader stack properly hadnles this
        }

        /**
         * Utility function to make error cases more concise.
         * @param condition the condition under which an IonException should be thrown
         * @param errorMessage the message to use in the exception
         */
        private void errorIf(boolean condition, String errorMessage) {
            if (condition) {
                throw new IonException(errorMessage);
            }
        }

        /**
         * Read an encoding directive. If the stream ends before the encoding directive finishes, `event` will be
         * `NEEDS_DATA` and this method can be called again when more data is available.
         */
        public Event nextValue() {
            Event event;
            while (true) {
                switch (state) {
                    case ON_DIRECTIVE_SEXP:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        state = EncodingDirectiveState.IN_DIRECTIVE_SEXP;
                        break;
                    case IN_DIRECTIVE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        errorIf(event == Event.END_CONTAINER, "invalid Ion directive; missing directive keyword");
                        classifyDirective();
                        break;
                    case IN_MODULE_DIRECTIVE_SEXP_AWAITING_MODULE_NAME:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        errorIf(event == Event.END_CONTAINER, "invalid module definition; missing module name");
                        errorIf(getEncodingType() != IonType.SYMBOL, "invalid module definition; module name must be a symbol");
                        // TODO: Support other module names
                        errorIf(!DEFAULT_MODULE.equals(getSymbolText()), "IonJava currently supports only the default module");
                        state = EncodingDirectiveState.IN_MODULE_DIRECTIVE_SEXP_BODY;
                        break;
                    case IN_MODULE_DIRECTIVE_SEXP_BODY:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishEncodingDirective();
                            popStackFrame();
                            return Event.NEEDS_INSTRUCTION;
                        }
                        if (getEncodingType() != IonType.SEXP) {
                            throw new IonException("module definitions must contain only s-expressions.");
                        }
                        state = EncodingDirectiveState.ON_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case ON_SEXP_IN_MODULE_DIRECTIVE:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        state = EncodingDirectiveState.IN_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case IN_SEXP_IN_MODULE_DIRECTIVE:
                        if (Event.NEEDS_DATA == coreNextValue()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (!IonType.isText(getEncodingType())) {
                            throw new IonException("S-expressions within module definitions must begin with a text token.");
                        }
                        state = EncodingDirectiveState.CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE;
                        break;
                    case CLASSIFYING_SEXP_IN_MODULE_DIRECTIVE:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        classifySexpWithinModuleDirective();
                        break;
                    case IN_SYMBOL_TABLE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        classifySymbolTable();
                        break;
                    case IN_APPENDED_SYMBOL_TABLE:
                        event = coreNextValue();
                        if (Event.NEEDS_DATA == event) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        isSymbolTableAppend = true;
                        if (Event.END_CONTAINER == event) {
                            // Nothing to append.
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        if (getEncodingType() != IonType.LIST) {
                            throw new IonException("symbol_table s-expression must begin with a list.");
                        }
                        state = EncodingDirectiveState.ON_SYMBOL_TABLE_LIST;
                        break;
                    case ON_SYMBOL_TABLE_LIST:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        state = EncodingDirectiveState.IN_SYMBOL_TABLE_LIST;
                        break;
                    case IN_SYMBOL_TABLE_LIST:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfContainer();
                            state = EncodingDirectiveState.IN_SYMBOL_TABLE_SEXP;
                            break;
                        }
                        if (!IonType.isText(getEncodingType())) {
                            throw new IonException("The symbol_table must contain text.");
                        }
                        state = EncodingDirectiveState.ON_SYMBOL;
                        break;
                    case ON_SYMBOL:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        newSymbols.add(stringValue());
                        state = EncodingDirectiveState.IN_SYMBOL_TABLE_LIST;
                        break;
                    case IN_MACRO_TABLE_SEXP:
                        event = coreNextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        }
                        classifyMacroTable();
                        break;
                    case IN_APPENDED_MACRO_TABLE:
                        event = coreNextValue();
                        if (Event.NEEDS_DATA == event) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        isMacroTableAppend = true;
                        if (event == Event.END_CONTAINER) {
                            // Nothing to append
                            stepOutOfSexpWithinEncodingDirective();
                            break;
                        } if (getEncodingType() != IonType.SEXP) {
                        throw new IonException("macro_table s-expression must contain s-expressions.");
                    }
                        state = EncodingDirectiveState.ON_MACRO_SEXP;
                        break;
                    case ON_MACRO_SEXP:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        state = EncodingDirectiveState.COMPILING_MACRO;
                        Macro newMacro = macroCompiler.compileMacro();
                        newMacros.put(MacroRef.byId(++localMacroMaxOffset), newMacro);
                        String macroName = macroCompiler.getMacroName();
                        if (macroName != null) {
                            newMacros.put(MacroRef.byName(macroName), newMacro);
                        }
                        state = EncodingDirectiveState.IN_MACRO_TABLE_SEXP;
                        break;
                    default:
                        throw new IllegalStateException(state.toString());
                }
            }
        }
    }

    /**
     * The reader's state. `READING_VALUE` indicates that the reader is reading a user-level value; all other states
     * indicate that the reader is in the middle of reading a symbol table.
     */
    private enum SymbolTableState {
        ON_SYMBOL_TABLE_STRUCT,
        ON_SYMBOL_TABLE_FIELD,
        ON_SYMBOL_TABLE_SYMBOLS,
        READING_SYMBOL_TABLE_SYMBOLS_LIST,
        READING_SYMBOL_TABLE_SYMBOL,
        ON_SYMBOL_TABLE_IMPORTS,
        READING_SYMBOL_TABLE_IMPORTS_LIST,
        READING_SYMBOL_TABLE_IMPORT_STRUCT,
        READING_SYMBOL_TABLE_IMPORT_NAME,
        READING_SYMBOL_TABLE_IMPORT_VERSION,
        READING_SYMBOL_TABLE_IMPORT_MAX_ID,
        READING_VALUE // TODO still necessary?
    }

    private class SymbolTableInterceptingIonCursor extends DelegatingIonReaderContinuableApplication {

        // The current state.
        private SymbolTableState state = SymbolTableState.READING_VALUE;

        private boolean hasSeenImports;
        private boolean hasSeenSymbols;
        private String name = null;
        private int version = -1;
        private int maxId = -1;
        private List<SymbolTable> newImports = null;
        private List<String> newSymbols = null;

        // TODO: note: if any child frames are pushed, they should not yield by default
        SymbolTableInterceptingIonCursor initialize(IonReaderContinuableApplication delegate) {
            setDelegate(delegate);
            hasSeenImports = false;
            hasSeenSymbols = false;
            newImports = null;
            newSymbols = null;
            name = null;
            version = -1;
            maxId = -1;
            state = SymbolTableState.ON_SYMBOL_TABLE_STRUCT;
            cachedReadOnlySymbolTable = null;
            return this;
        }

        private boolean valueUnavailable() {
            Event event = fillValue();
            return event == Event.NEEDS_DATA || event == Event.NEEDS_INSTRUCTION;
        }

        private void finishReadingSymbolTableStruct() {
            stepOutOfContainer();
            if (!hasSeenImports) {
                resetSymbolTable();
                resetImports(getIonMajorVersion(), getIonMinorVersion());
            }
            installSymbols(newSymbols);
            state = SymbolTableState.READING_VALUE;
        }

        /**
         * Gets the symbol ID for the Marker representing a symbol token.
         * @param marker the symbol token marker.
         * @return a symbol ID, or -1 if unknown or not a system symbol.
         */
        private int mapInlineTextToSystemSid(Marker marker) {
            if (marker.startIndex < 0) {
                // Symbol ID is already populated.
                return (int) marker.endIndex;
            }
            if (bytesMatch(SYMBOLS_UTF8, rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex)) {
                return SYMBOLS_SID;
            }
            if (bytesMatch(IMPORTS_UTF8, rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex)) {
                return IMPORTS_SID;
            }
            if (bytesMatch(NAME_UTF8, rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex)) {
                return NAME_SID;
            }
            if (bytesMatch(VERSION_UTF8, rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex)) {
                return VERSION_SID;
            }
            if (bytesMatch(MAX_ID_UTF8, rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex)) {
                return MAX_ID_SID;
            }
            // Not a system symbol.
            return -1;
        }

        private void readSymbolTableStructField() {
            if (rawCursor.minorVersion > 0) {
                readSymbolTableStructField_1_1();
                return;
            }
            if (rawCursor.fieldSid == SYMBOLS_SID) {
                state = SymbolTableState.ON_SYMBOL_TABLE_SYMBOLS;
                if (hasSeenSymbols) {
                    throw new IonException("Symbol table contained multiple symbols fields.");
                }
                hasSeenSymbols = true;
            } else if (rawCursor.fieldSid == IMPORTS_SID) {
                state = SymbolTableState.ON_SYMBOL_TABLE_IMPORTS;
                if (hasSeenImports) {
                    throw new IonException("Symbol table contained multiple imports fields.");
                }
                hasSeenImports = true;
            }
        }

        private void readSymbolTableStructField_1_1() {
            if (matchesSystemSymbol_1_1(rawCursor.fieldTextMarker, SystemSymbols_1_1.SYMBOLS)) {
                state = SymbolTableState.ON_SYMBOL_TABLE_SYMBOLS;
                if (hasSeenSymbols) {
                    throw new IonException("Symbol table contained multiple symbols fields.");
                }
                hasSeenSymbols = true;
            } else if (matchesSystemSymbol_1_1(rawCursor.fieldTextMarker, SystemSymbols_1_1.IMPORTS)) {
                state = SymbolTableState.ON_SYMBOL_TABLE_IMPORTS;
                if (hasSeenImports) {
                    throw new IonException("Symbol table contained multiple imports fields.");
                }
                hasSeenImports = true;
            }
        }

        private void startReadingImportsList() {
            resetImports(getIonMajorVersion(), getIonMinorVersion());
            resetSymbolTable();
            newImports = new ArrayList<>(3);
            if (rawCursor.minorVersion == 0) {
                newImports.add(getSystemSymbolTable(0)); // TODO check
            }
            state = SymbolTableState.READING_SYMBOL_TABLE_IMPORTS_LIST;
        }

        private void preparePossibleAppend() {
            if (rawCursor.minorVersion > 0) {
                rawCursor.prepareScalar(); // TODO check, could have been overridden in previous implementation
                if (!matchesSystemSymbol_1_1(rawCursor.valueMarker, SystemSymbols_1_1.ION_SYMBOL_TABLE)) {
                    resetSymbolTable();
                }
            } else {
                if (symbolValueId() != ION_SYMBOL_TABLE_SID) {
                    resetSymbolTable();
                }
            }
            state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
        }

        private void finishReadingImportsList() {
            stepOutOfContainer();
            imports = new LocalSymbolTableImports(newImports);
            firstLocalSymbolId = imports.getMaxId() + 1;
            state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingSymbolsList() {
            newSymbols = new ArrayList<>(8);
            state = SymbolTableState.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void startReadingSymbol() {
            if (delegate.getType() == IonType.STRING) {
                state = SymbolTableState.READING_SYMBOL_TABLE_SYMBOL;
            } else {
                newSymbols.add(null);
            }
        }

        private void finishReadingSymbol() {
            newSymbols.add(stringValue());
            state = SymbolTableState.READING_SYMBOL_TABLE_SYMBOLS_LIST;
        }

        private void finishReadingSymbolsList() {
            stepOutOfContainer();
            state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
        }

        private void startReadingImportStruct() {
            name = null;
            version = 1;
            maxId = -1;
            if (delegate.getType() == IonType.STRUCT) {
                stepIntoContainer();
                state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_STRUCT;
            }
        }

        private void finishReadingImportStruct() {
            stepOutOfContainer();
            state = SymbolTableState.READING_SYMBOL_TABLE_IMPORTS_LIST;
            // Ignore import clauses with malformed name field.
            if (name == null || name.length() == 0 || name.equals(ION)) {
                return;
            }
            newImports.add(createImport(name, version, maxId));
        }

        private void startReadingImportStructField() {
            int fieldId = getFieldId();
            if (rawCursor.minorVersion > 0 && fieldId < 0) {
                fieldId = mapInlineTextToSystemSid(rawCursor.fieldTextMarker);
            }
            if (fieldId == NAME_SID) {
                state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_NAME;
            } else if (fieldId == VERSION_SID) {
                state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_VERSION;
            } else if (fieldId == MAX_ID_SID) {
                state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_MAX_ID;
            }
        }

        private void readImportName() {
            if (delegate.getType() == IonType.STRING) {
                name = stringValue();
            }
            state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportVersion() {
            if (delegate.getType() == IonType.INT && !delegate.isNullValue()) {
                version = Math.max(1, intValue());
            }
            state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        private void readImportMaxId() {
            if (delegate.getType() == IonType.INT && !delegate.isNullValue()) {
                maxId = intValue();
            }
            state = SymbolTableState.READING_SYMBOL_TABLE_IMPORT_STRUCT;
        }

        @Override
        public Event nextValue() {
            Event event;
            while (true) {
                switch (state) {
                    case ON_SYMBOL_TABLE_STRUCT:
                        if (Event.NEEDS_DATA == stepIntoContainer()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
                        break;
                    case ON_SYMBOL_TABLE_FIELD:
                        event = delegate.nextValue();
                        if (Event.NEEDS_DATA == event) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolTableStruct();
                            popStackFrame();
                            return Event.NEEDS_INSTRUCTION;
                        }
                        readSymbolTableStructField();
                        break;
                    case ON_SYMBOL_TABLE_SYMBOLS:
                        if (delegate.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                yield = true;
                                return Event.NEEDS_DATA;
                            }
                            startReadingSymbolsList();
                        } else {
                            state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case ON_SYMBOL_TABLE_IMPORTS:
                        if (delegate.getType() == IonType.LIST) {
                            if (Event.NEEDS_DATA == stepIntoContainer()) {
                                yield = true;
                                return Event.NEEDS_DATA;
                            }
                            startReadingImportsList();
                        } else if (delegate.getType() == IonType.SYMBOL) {
                            if (valueUnavailable()) {
                                yield = true;
                                return Event.NEEDS_DATA;
                            }
                            preparePossibleAppend();
                        } else {
                            state = SymbolTableState.ON_SYMBOL_TABLE_FIELD;
                        }
                        break;
                    case READING_SYMBOL_TABLE_SYMBOLS_LIST:
                        event = delegate.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingSymbolsList();
                            break;
                        }
                        startReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_SYMBOL:
                        if (valueUnavailable()) {
                            return Event.NEEDS_DATA;
                        }
                        finishReadingSymbol();
                        break;
                    case READING_SYMBOL_TABLE_IMPORTS_LIST:
                        event = delegate.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportsList();
                            break;
                        }
                        startReadingImportStruct();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_STRUCT:
                        event = delegate.nextValue();
                        if (event == Event.NEEDS_DATA) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        if (event == Event.END_CONTAINER) {
                            finishReadingImportStruct();
                            break;
                        } else if (event != Event.START_SCALAR) {
                            break;
                        }
                        startReadingImportStructField();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_NAME:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        readImportName();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_VERSION:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        readImportVersion();
                        break;
                    case READING_SYMBOL_TABLE_IMPORT_MAX_ID:
                        if (valueUnavailable()) {
                            yield = true;
                            return Event.NEEDS_DATA;
                        }
                        readImportMaxId();
                        break;
                    default: throw new IllegalStateException(state.toString());
                }
            }
        }
    }

    private static final Iterator<String> EMPTY_ITERATOR = new Iterator<String>() {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public String next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove from an empty iterator.");
        }
    };

    private class SymbolResolvingIonCursor extends DelegatingIonReaderContinuableCore implements IonReaderContinuableApplication {

        private ParsingIonCursorBinary cursor;

        // The reusable annotation iterator.
        private final AnnotationMarkerIterator annotationTextIterator = new AnnotationMarkerIterator();

        SymbolResolvingIonCursor initialize(ParsingIonCursorBinary cursor) {
            setDelegate(cursor);
            this.cursor = cursor;
            annotationTextIterator.nextAnnotationPeekIndex = 0;
            return this;
        }

        @Override
        public EncodingContext getEncodingContext() { // TODO should this just be overridden at the Interpreter level and removed here and in BytecodeCursor?
            return encodingContext;
        }

        /**
         * Reusable iterator over the annotations on the current value.
         */
        private class AnnotationMarkerIterator implements Iterator<String> {

            // TODO perf: try splitting into separate iterators for SIDs and FlexSyms
            boolean isSids;
            // The byte position of the annotation to return from the next call to next().
            long nextAnnotationPeekIndex;

            long target;

            @Override
            public boolean hasNext() {
                return nextAnnotationPeekIndex < target;
            }

            @Override
            public String next() {
                if (isSids) {
                    long savedPeekIndex = cursor.peekIndex;
                    cursor.peekIndex = nextAnnotationPeekIndex;
                    int sid;
                    if (cursor.minorVersion == 0) {
                        byte b = cursor.buffer[(int) cursor.peekIndex++];
                        if (b < 0) {
                            sid = b & 0x7F;
                        } else {
                            sid = cursor.readVarUInt_1_0(b);
                        }
                    } else {
                        sid = (int) cursor.readFlexInt_1_1();
                    }
                    nextAnnotationPeekIndex = cursor.peekIndex;
                    cursor.peekIndex = savedPeekIndex;
                    return convertToString(sid);
                }
                Marker marker = cursor.annotationTokenMarkers.get((int) nextAnnotationPeekIndex++);
                if (marker.startIndex < 0) {
                    if (marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                        return cursor.getSystemSymbolToken(marker).assumeText();
                    } else {
                        // This means the endIndex represents the token's symbol ID.
                        return convertToString((int) marker.endIndex);
                    }
                }
                // The token is inline UTF-8 text.
                java.nio.ByteBuffer utf8InputBuffer = cursor.prepareByteBuffer(marker.startIndex, marker.endIndex);
                return cursor.utf8Decoder.decode(utf8InputBuffer, (int) (marker.endIndex - marker.startIndex));
            }

            SymbolToken nextSymbolToken() {
                if (isSids) {
                    long savedPeekIndex = cursor.peekIndex;
                    cursor.peekIndex = nextAnnotationPeekIndex;
                    int sid = cursor.minorVersion == 0 ? cursor.readVarUInt_1_0() : (int) cursor.readFlexInt_1_1();
                    nextAnnotationPeekIndex = cursor.peekIndex;
                    cursor.peekIndex = savedPeekIndex;
                    return getSymbolToken(sid);
                }
                Marker marker = cursor.annotationTokenMarkers.get((int) nextAnnotationPeekIndex++);
                if (marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                    if (marker.startIndex < 0) {
                        return cursor.getSystemSymbolToken(marker);
                    } else {
                        throw new IllegalStateException("This should be unreachable.");
                    }
                }
                if (marker.startIndex < 0) {
                    // This means the endIndex represents the token's symbol ID.
                    return getSymbolToken((int) marker.endIndex);
                }
                // The token is inline UTF-8 text.
                ByteBuffer utf8InputBuffer = cursor.prepareByteBuffer(marker.startIndex, marker.endIndex);
                return new SymbolTokenImpl(cursor.utf8Decoder.decode(utf8InputBuffer, (int) (marker.endIndex - marker.startIndex)), -1);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("This iterator does not support element removal.");
            }

            private String convertToString(int symbolId) {
                String annotation = getSymbol(symbolId);
                if (annotation == null) {
                    throw new UnknownSymbolException(symbolId);
                }
                return annotation;
            }
        }

        @Override
        public SymbolTable getSymbolTable() {
            return Interpreter.super.getSymbolTable();
        }

        @Override
        public String[] getTypeAnnotations() {
            if (!cursor.hasAnnotations) {
                return _Private_Utils.EMPTY_STRING_ARRAY;
            }
            if (cursor.annotationSequenceMarker.startIndex >= 0) {
                if (cursor.annotationSequenceMarker.typeId != null && cursor.annotationSequenceMarker.typeId.isInlineable) {
                    cursor.getAnnotationMarkerList();
                } else {
                    IntList annotationSids = cursor.getAnnotationSidList();
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
            }
            String[] annotationArray = new String[cursor.annotationTokenMarkers.size()];
            annotationTextIterator.nextAnnotationPeekIndex = 0;
            annotationTextIterator.target = cursor.annotationTokenMarkers.size();
            annotationTextIterator.isSids = false;
            while (annotationTextIterator.hasNext()) {
                annotationArray[(int) annotationTextIterator.nextAnnotationPeekIndex] = annotationTextIterator.next();
            }
            return annotationArray;
        }

        @Override
        public SymbolToken[] getTypeAnnotationSymbols() {
            if (!cursor.hasAnnotations) {
                return SymbolToken.EMPTY_ARRAY;
            }
            if (cursor.annotationSequenceMarker.startIndex >= 0) {
                if (cursor.annotationSequenceMarker.typeId != null && cursor.annotationSequenceMarker.typeId.isInlineable) {
                    cursor.getAnnotationMarkerList();
                } else {
                    IntList annotationSids = cursor.getAnnotationSidList();
                    SymbolToken[] annotationArray = new SymbolToken[annotationSids.size()];
                    for (int i = 0; i < annotationArray.length; i++) {
                        annotationArray[i] = getSymbolToken(annotationSids.get(i));
                    }
                    return annotationArray;
                }
            }
            SymbolToken[] annotationArray = new SymbolToken[cursor.annotationTokenMarkers.size()];
            annotationTextIterator.nextAnnotationPeekIndex = 0;
            annotationTextIterator.target = cursor.annotationTokenMarkers.size();
            annotationTextIterator.isSids = false;
            while (annotationTextIterator.hasNext()) {
                annotationArray[(int) annotationTextIterator.nextAnnotationPeekIndex] = annotationTextIterator.nextSymbolToken();
            }
            return annotationArray;
        }

        @Override
        public Iterator<String> iterateTypeAnnotations() {
            if (!cursor.hasAnnotations) {
                return EMPTY_ITERATOR;
            }
            if (cursor.annotationSequenceMarker.startIndex >= 0) {
                if (cursor.annotationSequenceMarker.typeId != null && cursor.annotationSequenceMarker.typeId.isInlineable) {
                    // Note: this could be made more efficient by parsing from the marker sequence iteratively.
                    cursor.getAnnotationMarkerList();
                } else {
                    annotationTextIterator.nextAnnotationPeekIndex = cursor.annotationSequenceMarker.startIndex;
                    annotationTextIterator.target = cursor.annotationSequenceMarker.endIndex;
                    annotationTextIterator.isSids = true;
                    return annotationTextIterator;
                }
            }
            annotationTextIterator.nextAnnotationPeekIndex = 0;
            annotationTextIterator.target = cursor.annotationTokenMarkers.size();
            annotationTextIterator.isSids = false;
            return annotationTextIterator;
        }

        @Override
        public String getFieldName() {
            if (cursor.fieldTextMarker.startIndex > -1 || cursor.fieldTextMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                return getFieldText();
            }
            if (cursor.fieldSid < 0) {
                return null;
            }
            String fieldName = getSymbol(cursor.fieldSid);
            if (fieldName == null) {
                throw new UnknownSymbolException(cursor.fieldSid);
            }
            return fieldName;
        }

        /**
         * Creates a SymbolToken representation of the given symbol ID.
         *
         * @param sid a symbol ID.
         * @return a SymbolToken.
         */
        protected SymbolToken getSymbolToken(int sid) {
            return new SymbolTokenImpl(getSymbol(sid), sid);
        }

        @Override
        public SymbolToken getFieldNameSymbol() {
            if (cursor.fieldTextMarker.startIndex > -1) {
                return new SymbolTokenImpl(getFieldText(), SymbolTable.UNKNOWN_SYMBOL_ID);
            }
            if (cursor.fieldTextMarker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                return cursor.getSystemSymbolToken(cursor.fieldTextMarker);
            }
            if (cursor.fieldSid < 0) {
                return null;
            }
            return getSymbolToken(cursor.fieldSid);
        }

        @Override
        public String stringValue() {
            String value;
            IonType type = getEncodingType();
            if (type == IonType.STRING) {
                value = cursor.readString();
            } else if (type == IonType.SYMBOL) {
                if (cursor.valueTid.isInlineable) {
                    value = cursor.readString();
                } else if (cursor.valueTid == IonTypeID.SYSTEM_SYMBOL_VALUE) {
                    value = getSymbolText();
                } else {
                    int sid = cursor.symbolValueId();
                    if (sid < 0) {
                        // The raw reader uses this to denote null.symbol.
                        return null;
                    }
                    value = getSymbol(sid);
                    if (value == null) {
                        throw new UnknownSymbolException(sid);
                    }
                }
            } else {
                throw new IllegalStateException("Invalid type requested.");
            }
            return value;
        }

        @Override
        public String getSymbol(int sid) {
            // TODO to support system-level transcode, it needs to be possible to omit the symbol-resolving cursor.
            //  In that case, the only symbols that should be resolved are the system symbols (for readability).
            /*
            // Only symbol IDs declared in Ion 1.1 encoding directives (not Ion 1.0 symbol tables) are resolved by the
            // core reader. In Ion 1.0, 'symbols' is never populated by the core reader.
            if (sid > 0 && sid - 1 <= localSymbolMaxOffset) {
                return symbols[sid - 1];
            }
            return null;

             */
            if (sid < firstLocalSymbolId) {
                return imports.findKnownSymbol(sid);
            }
            int localSymbolOffset = sid - firstLocalSymbolId;
            if (localSymbolOffset > localSymbolMaxOffset) {
                throw new UnknownSymbolException(sid);
            }
            return symbols[localSymbolOffset];
        }
    }

    private class ApplicationLevelIonCursor extends DelegatingIonReaderContinuableApplication {

        private final SymbolTableInterceptingIonCursor symbolTableInterceptingIonCursor = new SymbolTableInterceptingIonCursor();
        private final DepthOneIonCursor depthOneCursor = new DepthOneIonCursor();
        private IonReaderContinuableApplication core = null;

        // TODO should this accept an EncodingDirectiveAwareIonCursor?
        // 'core' is the next-highest-level cursor below the encoding directive-aware cursor (if any). If the encoding
        // directive-aware cursor isn't used, then 'delegate' and 'core' will be the same.
        ApplicationLevelIonCursor initialize(IonReaderContinuableApplication delegate, IonReaderContinuableApplication core) {
            setDelegate(delegate);
            this.core = core;
            return this;
        }

        /**
         * @return true if current value has a sequence of annotations that begins with `$ion_symbol_table`; otherwise,
         *  false.
         */
        protected boolean startsWithIonSymbolTable() {
            if (rawCursor.minorVersion == 0 && rawCursor.annotationSequenceMarker.startIndex >= 0) {
                long savedPeekIndex = rawCursor.peekIndex; // TODO check, might have been the core reader's peekIndex in previous implementation
                rawCursor.peekIndex = rawCursor.annotationSequenceMarker.startIndex;
                int sid = rawCursor.readVarUInt_1_0();
                rawCursor.peekIndex = savedPeekIndex;
                return ION_SYMBOL_TABLE_SID == sid;
            } else if (rawCursor.minorVersion == 1) {
                Marker marker = rawCursor.annotationTokenMarkers.get(0);
                return matchesSystemSymbol_1_1(marker, SystemSymbols_1_1.ION_SYMBOL_TABLE);
            }
            return false;
        }

        /**
         * @return true if the reader is positioned on a symbol table; otherwise, false.
         */
        protected boolean isPositionedOnSymbolTable() {
            return rawCursor.hasAnnotations && // TODO here and elsewhere in this class, rawCursor usage is questionable. Could have a symbol table created from an e-expression invocation.
                getEncodingType() == IonType.STRUCT &&
                startsWithIonSymbolTable();
        }

        @Override
        public Event nextValue() {
            Event event = delegate.nextValue();
            // TODO check for symbol table. If yes, push onto the stack the encoding directive reader and don't yield
            if (isPositionedOnSymbolTable()) {
                pushStackFrame().initializeDataSource(symbolTableInterceptingIonCursor.initialize(core));
                top.yieldByDefault = false;
                event = Event.NEEDS_INSTRUCTION;
            }
            return event;
        }

        @Override
        public Event stepIntoContainer() {
            pushStackFrame().initializeDataSource(depthOneCursor.initialize(core)); // TODO argumentSource?
            return core.stepIntoContainer();
        }

    }

    private class DepthOneIonCursor extends DelegatingIonReaderContinuableApplication {

        DepthOneIonCursor initialize(IonReaderContinuableApplication delegate) {
            setDelegate(delegate);
            return this;
        }

        @Override
        public Event stepOutOfContainer() {
            delegate.stepOutOfContainer();
            popStackFrame();
            return Event.NEEDS_INSTRUCTION;
        }
    }

    private class MacroAwareIonCursorBinary extends DelegatingIonReaderContinuableApplication {

        private SymbolResolvingIonCursor dataSource;
        private ParsingIonCursorBinary cursor;

        MacroAwareIonCursorBinary initialize(SymbolResolvingIonCursor delegate) {
            dataSource = delegate;
            this.cursor = delegate.cursor;
            setDelegate(delegate);
            return this;
        }

        private Macro loadMacro() {
            long macroAddress = cursor.getMacroInvocationId();
            boolean isSystemMacro = cursor.isSystemInvocation();
            Macro macro;
            if (isSystemMacro) {
                macro = SystemMacro.get((int) macroAddress);
            } else {
                if (macroAddress > Integer.MAX_VALUE) {
                    throw new IonException("Macro addresses larger than 2147483647 are not supported by this implementation.");
                }
                MacroRef address = MacroRef.byId((int) macroAddress);
                macro = encodingContext.getMacroTable().get(address);

                if (macro == null) {
                    throw new IonException(String.format("Encountered an unknown macro address: %d.", macroAddress));
                }
            }
            return macro;
        }

        @Override
        public Event nextValue() {
            Event event = cursor.nextValue();
            if (event == Event.NEEDS_INSTRUCTION) {
                if (cursor.valueTid != null && cursor.valueTid.isMacroInvocation) {
                    Macro macro = loadMacro();
                    Bytecode bodyBytecode = macro.getBodyBytecode();
                    cursor.stepIntoEExpression();
                    presenceBitmapPool.clear(); // TODO is there a better place for this?
                    PresenceBitmap presenceBitmap = cursor.loadPresenceBitmap(macro.getSignature(), presenceBitmapPool);
                    pushStackFrame()
                        .initializeBytecodeDataSource(bodyBytecode, 0, bodyBytecode.size(), true, null, null)
                        .initializeLazyArgumentSource(presenceBitmap, dataSource, macro.getSignature());
                }
            }
            return event;
        }

        @Override
        public void close() throws IOException {
            // Do nothing
        }
    }

    private class BytecodeCursor implements IonReaderContinuableApplication {

        private IonType ionType = null;
        private Iterator<String> annotations = Collections.emptyIterator();
        private String initialFieldName = null;
        private String fieldName = null;

        // Holders for already-materialized values
        private boolean isNull = false;
        private boolean booleanValue;
        private int intValue;
        private long longValue;
        private double floatValue;
        private BigInteger bigIntValue;
        private BigDecimal bigDecimalValue;
        private Timestamp timestampValue;
        private String stringValue;
        private IntegerSize integerSize;

        private final BytecodeArgumentSource argumentSource = new BytecodeArgumentSource();
        private ArgumentSource parentArguments;
        private Bytecode bytecode = null;
        private int programCounter;
        private int programCounterLimit;
        private boolean popStackWhenExhausted;

        public BytecodeCursor initialize(Bytecode bytecode, int programCounterStart, int programCounterLimit, boolean popStackWhenExhausted, ArgumentSource parentArguments, String fieldName) {
            this.bytecode = bytecode;
            programCounter = programCounterStart;
            this.programCounterLimit = programCounterLimit;
            this.popStackWhenExhausted = popStackWhenExhausted;
            this.parentArguments = parentArguments;
            this.initialFieldName = fieldName;
            return this;
        }

        private int nextOpcode() {
            return bytecode.getOpcode(programCounter++);
        }

        private void reset() {
            isNull = false;
            fieldName = initialFieldName;
            initialFieldName = null;
            annotations = Collections.emptyIterator();
            ionType = null;
            bigIntValue = null;
            bigDecimalValue = null;
            timestampValue = null;
            stringValue = null;
            argumentSource.initialize(null/*, null*/);
            integerSize = null;
        }

        @Override
        public Event nextValue() {
            reset();
            while (programCounter < programCounterLimit) {
                int opcode = nextOpcode();
                int operation = (opcode >>> OPERATION_RIGHT_SHIFT) & ONE_BYTE_MASK;
                switch (operation) {
                    case BytecodeOpcodes.OP_NULL_NULL:
                        ionType = IonType.NULL;
                        isNull = true;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_NULL_TYPED:
                        ionType = ION_TYPES_BY_ORDINAL[opcode & DATA_MASK];
                        isNull = true;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_BOOL:
                        ionType = IonType.BOOL;
                        booleanValue = (opcode & DATA_MASK) != 0;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_SMALL_INT:
                        ionType = IonType.INT;
                        intValue = opcode & DATA_MASK;
                        integerSize = IntegerSize.INT;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_INLINE_INT:
                        ionType = IonType.INT;
                        intValue = nextOpcode();
                        integerSize = IntegerSize.INT;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_LONG:
                        ionType = IonType.INT;
                        integerSize = IntegerSize.LONG;
                        longValue = bytecode.getLong(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_BIG_INT:
                        ionType = IonType.INT;
                        integerSize = IntegerSize.BIG_INTEGER;
                        bigIntValue = bytecode.getBigInteger(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_SMALL_NEG_INT:
                        ionType = IonType.INT;
                        intValue = -(opcode & DATA_MASK);
                        integerSize = IntegerSize.INT;
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_FLOAT:
                        ionType = IonType.FLOAT;
                        floatValue = bytecode.getDouble(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_DECIMAL:
                        ionType = IonType.DECIMAL;
                        bigDecimalValue = bytecode.getBigDecimal(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_TIMESTAMP:
                        ionType = IonType.TIMESTAMP;
                        timestampValue = bytecode.getTimestamp(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_STRING:
                        ionType = IonType.STRING;
                        stringValue = bytecode.getString(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_EMPTY_STRING:
                        ionType = IonType.STRING;
                        stringValue = "";
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_CP_SYMBOL:
                        ionType = IonType.SYMBOL;
                        stringValue = bytecode.getString(opcode & DATA_MASK);
                        return IonCursor.Event.START_SCALAR;
                    case BytecodeOpcodes.OP_EMPTY_SYMBOL:
                        ionType = IonType.SYMBOL;
                        stringValue = "";
                        return IonCursor.Event.START_SCALAR;
                    // TODO blob
                    // TODO clob
                    // TODO annotation(s)
                    case BytecodeOpcodes.OP_STRUCT_START:
                        ionType = IonType.STRUCT;
                        // TODO push to stack?
                        return IonCursor.Event.START_CONTAINER;
                    case BytecodeOpcodes.OP_LIST_START:
                        ionType = IonType.LIST;
                        // TODO push to stack?
                        return IonCursor.Event.START_CONTAINER;
                    case BytecodeOpcodes.OP_SEXP_START:
                        // TODO push to stack?
                        ionType = IonType.SEXP;
                        return IonCursor.Event.START_CONTAINER;
                    case BytecodeOpcodes.OP_STRUCT_END:
                    case BytecodeOpcodes.OP_LIST_END:
                    case BytecodeOpcodes.OP_SEXP_END:
                        // TODO pop from stack?
                        return IonCursor.Event.END_CONTAINER;
                    case BytecodeOpcodes.OP_CP_FIELD_NAME:
                        fieldName = bytecode.getString(opcode & DATA_MASK);
                        break;
                    case BytecodeOpcodes.OP_ARGUMENT_VALUE:
                        if (argumentSource.isNotInitialized()) {
                            argumentSource.initialize(bytecode/*, parentArguments*/);
                        }
                        int length = opcode & DATA_MASK;
                        argumentSource.addArgument(programCounter, programCounter + length, parentArguments);
                        break;
                    case BytecodeOpcodes.OP_ARGUMENT_REF_TYPE:
                        /*
                        if (argumentSource.isNotInitialized()) {
                            argumentSource.initialize(bytecode, parentArguments);
                        }
                        int argumentIndex = opcode & DATA_MASK;
                        argumentSource.addArgument(parentArguments, argumentIndex);
                        break;
                         */
                        int argumentIndex = opcode & DATA_MASK;
                        ArgumentSource topArgumentSource = top.argumentSource;
                        IonReaderContinuableApplication topDataSource = topArgumentSource.evaluateArgument(argumentIndex, fieldName);
                        pushStackFrame().initializeDataSource(topDataSource).initializeArgumentSource(topArgumentSource);
                        return Event.NEEDS_INSTRUCTION;
                    case BytecodeOpcodes.OP_INVOKE_MACRO:
                        Macro macro = bytecode.getMacro(opcode & DATA_MASK);
                        Bytecode bodyBytecode = macro.getBodyBytecode();
                        ArgumentSource parentArgumentSource = top.argumentSource;
                        pushStackFrame()
                            .initializeBytecodeDataSource(bodyBytecode, 0, bodyBytecode.size(), true, parentArgumentSource, fieldName)
                            .initializeArgumentSource(argumentSource);
                        return Event.NEEDS_INSTRUCTION;
                    case BytecodeOpcodes.OP_START_ARGUMENT_VALUE: // TODO expression group?
                    case BytecodeOpcodes.OP_END_EXPR_GROUP:
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            if (popStackWhenExhausted) {
                top.argumentSource.close();
                popStackFrame();
            }
            return Event.NEEDS_INSTRUCTION; // Note: conveys end of evaluation
        }

        @Override
        public Event stepIntoContainer() {
            return Event.NEEDS_INSTRUCTION; // TODO
        }

        @Override
        public Event stepOutOfContainer() {
            return Event.NEEDS_INSTRUCTION; // TODO
        }

        @Override
        public Event fillValue() {
            return Event.VALUE_READY;
        }

        @Override
        public Event getCurrentEvent() {
            return null;
        }

        @Override
        public Event endStream() {
            return Event.NEEDS_DATA;
        }

        @Override
        public void close() {
            // Nothing to do
        }

        @Override
        public int getDepth() {
            return 0; // TODO
        }

        @Override
        public IonType getType() {
            return ionType;
        }

        @Override
        public IonType getEncodingType() {
            return ionType;
        }

        @Override
        public IntegerSize getIntegerSize() {
            return integerSize;
        }

        @Override
        public boolean isNullValue() {
            return isNull;
        }

        @Override
        public boolean isInStruct() {
            return false; // TODO
        }

        @Override
        public int getFieldId() {
            return -1;
        }

        @Override
        public boolean hasFieldText() {
            return fieldName != null; // TODO or just true?
        }

        @Override
        public String getFieldText() {
            return fieldName;
        }

        @Override
        public SymbolToken getFieldNameSymbol() {
            if (fieldName == null) {
                return null;
            }
            return _Private_Utils.newSymbolToken(fieldName);
        }

        @Override
        public void consumeAnnotationTokens(Consumer<SymbolToken> consumer) {
            annotations.forEachRemaining(s -> consumer.accept(_Private_Utils.newSymbolToken(s)));
        }

        @Override
        public boolean booleanValue() {
            return booleanValue;
        }

        @Override
        public int intValue() {
            return intValue;
        }

        @Override
        public long longValue() {
            return longValue;
        }

        @Override
        public BigInteger bigIntegerValue() {
            return bigIntValue;
        }

        @Override
        public double doubleValue() {
            return floatValue;
        }

        @Override
        public BigDecimal bigDecimalValue() {
            return bigDecimalValue;
        }

        @Override
        public Decimal decimalValue() {
            return Decimal.valueOf(bigDecimalValue); // TODO optimize
        }

        @Override
        public Date dateValue() {
            return timestampValue.dateValue();
        }

        @Override
        public Timestamp timestampValue() {
            return timestampValue;
        }

        @Override
        public String stringValue() {
            return stringValue;
        }

        @Override
        public int symbolValueId() {
            return -1;
        }

        @Override
        public boolean hasSymbolText() {
            return ionType == IonType.SYMBOL;
        }

        @Override
        public String getSymbolText() {
            return stringValue;
        }

        @Override
        public SymbolToken symbolValue() {
            return _Private_Utils.newSymbolToken(stringValue);
        }

        @Override
        public int byteSize() {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public byte[] newBytes() {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public int getBytes(byte[] buffer, int offset, int len) {
            throw new UnsupportedOperationException(); // TODO
        }

        @Override
        public int getIonMajorVersion() {
            return 1;
        }

        @Override
        public int getIonMinorVersion() {
            return 1;
        }

        @Override
        public void registerIvmNotificationConsumer(IvmNotificationConsumer ivmConsumer) {
            // Nothing to do; bytecode is only used below the top level.
        }

        @Override
        public boolean hasAnnotations() {
            return annotations.hasNext();
        }

        @Override
        public void resetEncodingContext() {
            // TODO do nothing?
        }

        @Override
        public String getSymbol(int sid) {
            throw new UnsupportedOperationException(); // TODO does something need to be done for this? Symbol table lookup from the parent context?
        }

        @Override
        public EncodingContext getEncodingContext() {
            return encodingContext;
        }

        @Override
        public SymbolTable getSymbolTable() {
            return Interpreter.super.getSymbolTable();
        }

        @Override
        public String[] getTypeAnnotations() {
            // TODO inefficient
            List<String> annotationsList = new ArrayList<>(2);
            annotations.forEachRemaining(annotationsList::add);
            return annotationsList.toArray(EMPTY_STRING_ARRAY);
        }

        @Override
        public Iterator<String> iterateTypeAnnotations() {
            return annotations;
        }

        @Override
        public String getFieldName() {
            return fieldName;
        }

        @Override
        public SymbolToken[] getTypeAnnotationSymbols() {
            // TODO very inefficient
            List<SymbolToken> annotationsList = new ArrayList<>(2);
            annotations.forEachRemaining(s -> annotationsList.add(_Private_Utils.newSymbolToken(s)));
            return annotationsList.toArray(SymbolToken.EMPTY_ARRAY);
        }
    }

    /**
     * Read-only snapshot of the local symbol table at the reader's current position.
     */
    private class LocalSymbolTableSnapshot implements _Private_LocalSymbolTable, SymbolTableAsStruct {

        // The system symbol table.
        private final SymbolTable system = getSystemSymbolTable();

        // The max ID of this local symbol table.
        private final int maxId;

        // The shared symbol tables imported by this local symbol table.
        private final LocalSymbolTableImports importedTables;

        // Map representation of this symbol table. Keys are symbol text; values are the lowest symbol ID that maps
        // to that text.
        final Map<String, Integer> textToId;

        // List representation of this symbol table, indexed by symbol ID.
        final String[] idToText;

        private SymbolTableStructCache structCache = null;

        LocalSymbolTableSnapshot() {
            int importsMaxId = imports.getMaxId();
            int numberOfLocalSymbols = localSymbolMaxOffset + 1;
            // Note: 'imports' is immutable, so a clone is not needed.
            importedTables = imports;
            maxId = importsMaxId + numberOfLocalSymbols;
            idToText = new String[numberOfLocalSymbols];
            System.arraycopy(symbols, 0, idToText, 0, numberOfLocalSymbols);
            // Map with initial size and load factor set so that it will not grow unconditionally when it is filled.
            // Note: using the default load factor of 0.75 results in better lookup performance than using 1.0 and
            // filling the map to capacity.
            textToId = new HashMap<>((int) Math.ceil(numberOfLocalSymbols / 0.75), 0.75f);
            for (int i = 0; i < numberOfLocalSymbols; i++) {
                String symbol = idToText[i];
                if (symbol != null) {
                    textToId.put(symbol, i + importsMaxId + 1);
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
            Integer sid = textToId.get(text);
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
            return new SymbolTokenImpl(text, sid);
        }

        @Override
        public int findSymbol(String name) {
            Integer sid = importedTables.findSymbol(name);
            if (sid > UNKNOWN_SYMBOL_ID) {
                return sid;
            }
            sid = textToId.get(name);
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
            return getSymbolString(id, importedTables, idToText);
        }

        @Override
        public Iterator<String> iterateDeclaredSymbolNames() {
            return new Iterator<String>() {

                private int index = 0;

                @Override
                public boolean hasNext() {
                    return index < idToText.length;
                }

                @Override
                public String next() {
                    if (index >= idToText.length) {
                        throw new NoSuchElementException();
                    }
                    String symbol = idToText[index];
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
            IonReader reader = new com.amazon.ion.impl.SymbolTableReader(this);
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

        @Override
        public _Private_LocalSymbolTable makeCopy() {
            // This is a mutable copy. LocalSymbolTable handles the mutability concerns.
            return new LocalSymbolTable(importedTables, Arrays.asList(idToText));
        }

        @Override
        public SymbolTable[] getImportedTablesNoCopy() {
            return importedTables.getImportedTablesNoCopy();
        }
    }

    /**
     * Grows the `symbols` array to the next power of 2 that will fit the current need.
     */
    protected void growSymbolsArray(int shortfall) {
        int newSize = IonCursorBinary.nextPowerOfTwo(symbols.length + shortfall);
        String[] resized = new String[newSize];
        System.arraycopy(symbols, 0, resized, 0, localSymbolMaxOffset + 1);
        symbols = resized;
    }

    /**
     * Installs the given symbols at the end of the `symbols` array.
     * @param newSymbols the symbols to install.
     */
    protected void installSymbols(List<String> newSymbols) {
        if (newSymbols != null && !newSymbols.isEmpty()) {
            int numberOfNewSymbols = newSymbols.size();
            int numberOfAvailableSlots = symbols.length - (localSymbolMaxOffset + 1);
            int shortfall = numberOfNewSymbols - numberOfAvailableSlots;
            if (shortfall > 0) {
                growSymbolsArray(shortfall);
            }
            int i = localSymbolMaxOffset;
            for (String newSymbol : newSymbols) {
                symbols[++i] = newSymbol;
            }
            localSymbolMaxOffset += newSymbols.size();
        }
    }

    @Override
    public void resetEncodingContext() {
        resetSymbolTable();
        int minorVersion = getIonMinorVersion();
        resetImports(getIonMajorVersion(), minorVersion);
        if (minorVersion > 0) {
            // TODO reset macro table
            installSymbols(SystemSymbols_1_1.allSymbolTexts());
        }
    }

    //@Override
    protected void resetSymbolTable() {
        // super.resetSymbolTable(); // TODO?
        // The following line is not required for correctness, but it frees the references to the old symbols,
        // potentially allowing them to be garbage collected.
        Arrays.fill(symbols, 0, localSymbolMaxOffset + 1, null);
        localSymbolMaxOffset = -1;
        cachedReadOnlySymbolTable = null;
    }


    //@Override
    protected void resetImports(int major, int minor) {
        if (minor == 0) {
            imports = ION_1_0_IMPORTS;
        } else {
            imports = LocalSymbolTableImports.EMPTY;
        }
        firstLocalSymbolId = imports.getMaxId() + 1;
    }

    /**
     * Restore a symbol table from a previous point in the stream.
     * @param symbolTable the symbol table to restore.
     */
    protected void restoreSymbolTable(SymbolTable symbolTable) {
        if (cachedReadOnlySymbolTable == symbolTable) {
            return;
        }
        if (symbolTable instanceof LocalSymbolTableSnapshot) {
            LocalSymbolTableSnapshot snapshot = (LocalSymbolTableSnapshot) symbolTable;
            cachedReadOnlySymbolTable = snapshot;
            imports = snapshot.importedTables;
            firstLocalSymbolId = imports.getMaxId() + 1;
            // 'symbols' may be smaller than 'idToText' if the span was created from a different reader.
            int shortfall = snapshot.idToText.length - symbols.length;
            if (shortfall > 0) {
                growSymbolsArray(shortfall);
            }
            localSymbolMaxOffset = snapshot.maxId - firstLocalSymbolId;
            System.arraycopy(snapshot.idToText, 0, symbols, 0, snapshot.idToText.length);
        } else {
            // Note: this will only happen when `symbolTable` is the system symbol table.
            resetSymbolTable();
            cachedReadOnlySymbolTable = symbolTable;
            // FIXME: This should take into account the version at the point in the stream.
            resetImports(1, 0);
            localSymbolMaxOffset = -1;
        }
    }

    /**
     * Returns true if the symbol at `marker`...
     * <p> * is a system symbol with the same ID as the expected System Symbol
     * <p> * is an inline symbol with the same utf8 bytes as the expected System Symbol
     * <p> * is a user symbol that maps to the same text as the expected System Symbol
     * <p>
     */
    boolean matchesSystemSymbol_1_1(Marker marker, SystemSymbols_1_1 systemSymbol) {
        if (marker.typeId == IonTypeID.SYSTEM_SYMBOL_VALUE) {
            return systemSymbol.getText().equals(rawCursor.getSystemSymbolToken(marker).getText());
        } else if (marker.startIndex < 0) {
            // This is a local symbol whose ID is stored in marker.endIndex.
            return systemSymbol.getText().equals(getSymbol((int) marker.endIndex));
        } else {
            // This is an inline symbol with UTF-8 bytes bounded by the marker.
            return bytesMatch(systemSymbol.getUtf8Bytes(), rawCursor.buffer, (int) marker.startIndex, (int) marker.endIndex);
        }
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
        if (maxId < 0) {
            if (shared == null || version != shared.getVersion()) {
                String message =
                    "Import of shared table "
                        + name
                        + " lacks a valid max_id field, but an exact match was not"
                        + " found in the catalog";
                if (shared != null) {
                    message += " (found version " + shared.getVersion() + ")";
                }
                throw new IonException(message);
            }

            // Exact match is found, but max_id is undefined in import declaration. Set max_id to the largest SID of
            // the matching symbol table.
            maxId = shared.getMaxId();
        }
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
    private String getSymbolString(int sid, LocalSymbolTableImports importedSymbols, String[] localSymbols) {
        if (sid <= importedSymbols.getMaxId()) {
            return importedSymbols.findKnownSymbol(sid);
        }
        return localSymbols[sid - (importedSymbols.getMaxId() + 1)];
    }

    @Override
    public String getSymbol(int sid) {
        if (sid < firstLocalSymbolId) {
            return imports.findKnownSymbol(sid);
        }
        int localSymbolOffset = sid - firstLocalSymbolId;
        if (localSymbolOffset > localSymbolMaxOffset) {
            throw new UnknownSymbolException(sid);
        }
        return symbols[localSymbolOffset];
    }

    //@Override
    protected SymbolToken getSymbolToken(int sid) {
        int symbolTableSize = localSymbolMaxOffset + firstLocalSymbolId + 1; // +1 because the max ID is 0-indexed.
        if (sid >= symbolTableSize) {
            throw new UnknownSymbolException(sid);
        }
        String text = getSymbolString(sid, imports, symbols);
        if (text == null && sid >= firstLocalSymbolId) {
            // All symbols with unknown text in the local symbol range are equivalent to symbol zero.
            sid = 0;
        }
        return new SymbolTokenImpl(text, sid);
    }

    @Override
    public SymbolTable getSymbolTable() {
        if (cachedReadOnlySymbolTable == null) {
            if (localSymbolMaxOffset < 0 && imports == ION_1_0_IMPORTS) {
                cachedReadOnlySymbolTable = imports.getSystemSymbolTable();
            } else {
                cachedReadOnlySymbolTable = new LocalSymbolTableSnapshot();
            }
        }
        return cachedReadOnlySymbolTable;
    }

    @Override
    public EncodingContext getEncodingContext() {
        return encodingContext;
    }

    // The data source may either be an IonCursorBinary over the raw encoding or an interpreter over some bytecode
    private StackFrame pushStackFrame() {
        if (stackFrameIndex >= stack.length) {
            StackFrame[] newStack = new StackFrame[stack.length * 2];
            System.arraycopy(stack, 0, newStack, 0, stack.length);
            stack = newStack;
        }
        StackFrame stackFrame = stack[++stackFrameIndex];
        if (stackFrame == null) {
            stackFrame = new StackFrame();
            stack[stackFrameIndex] = stackFrame;
        }
        //stackFrameIndex++;
        stackFrame.argumentSource = null; //argumentSource;
        stackFrame.dataSource = null;
        //stackFrame.dataSource = dataSource;
        stackFrame.yieldByDefault = true;
        top = stackFrame;
        yield = false;
        return stackFrame;
    }

    private void popStackFrame() {
        if (--stackFrameIndex < 0) {
            top = null;
            return;
        }
        // TODO top.dataSource.close() ?
        top = stack[stackFrameIndex];
        setDelegate(top.dataSource);
        yield = false;
    }

    @Override
    public IonCursor.Event nextValue() {
        Event event;
        do {
            yield = top.yieldByDefault;
            event = top.dataSource.nextValue();
        } while (!yield);
        return event;
    }

    @Override
    public void close() {
        popStackFrame();
        rawCursor.close(); // TODO make sure this isn't already done
    }
}
