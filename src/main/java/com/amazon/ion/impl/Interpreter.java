package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonCursor;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.IvmNotificationConsumer;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.EncodingContext;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroRef;
import com.amazon.ion.impl.macro.SystemMacro;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Interpreter extends DelegatingIonReaderContinuableCore {

    private static final int OPERATION_RIGHT_SHIFT = 24;
    private static final int ONE_BYTE_MASK = 0xFF;
    private static final int DATA_MASK = 0x00FFFFFF;
    private static final IonType[] ION_TYPES_BY_ORDINAL = IonType.values();

    //private final IonCursorBinary cursor; // TODO would be nice to have an abstraction that could serve both binary and text
    private StackFrame[] stack = new StackFrame[8];
    private int stackFrameIndex = -1;
    private StackFrame top = null;
    private EncodingContext encodingContext = EncodingContext.getDefault();
    private boolean yield = true;

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    // TODO everywhere in this class, avoid new*, use pooling

    // TODO the goal is to have the interpreter be the only IonCursor implementation the Core reader interacts with,
    //  allowing it to remove the "isEvaluatingEExpression" checks from all its methods. This may require some of the
    //  core reader's value parsing methods to be moved in here so they can operate on the interpreter's raw cursor.
    Interpreter(ParsingIonCursorBinary cursor) {
        //this.cursor = cursor;
        pushStackFrame(new MacroAwareIonCursorBinary().initialize(cursor), null);
    }

    public void setEncodingContext(EncodingContext encodingContext) {
        this.encodingContext = encodingContext;
    }

    private static class StackFrame {

        private ArgumentSource argumentSource;
        private IonReaderContinuableCore dataSource;
    }

    private final IonReaderContinuableCore voidCursor = new DelegatingIonReaderContinuableCore() {

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

        public IonReaderContinuableCore evaluate(String fieldName) {
            if (source == null) {
                return new BytecodeCursor().initialize(bytecode, startIndex, endIndex, false, parentArguments, fieldName);
            }
            return source.evaluateArgument(startIndex, fieldName);
        }
    }

    private interface ArgumentSource {
        IonReaderContinuableCore evaluateArgument(int argumentIndex, String fieldName);
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
        public IonReaderContinuableCore evaluateArgument(int argumentIndex, String fieldName) {
            return argumentRegisters[argumentIndex].evaluate(fieldName);
        }

        @Override
        public void close() {
            // Nothing to do
        }
    }

    private class LazyArgumentSource implements ArgumentSource {

        private PresenceBitmap presenceBitmap;
        private ParsingIonCursorBinary dataSource;
        private List<Macro.Parameter> signature;
        private int firstArgumentStart;
        private int firstArgumentEnd;
        private IonTypeID firstArgumentTypeId;
        private int currentArgumentIndex;

        LazyArgumentSource initialize(PresenceBitmap presenceBitmap, ParsingIonCursorBinary dataSource, List<Macro.Parameter> signature) {
            this.presenceBitmap = presenceBitmap;
            this.dataSource = dataSource;
            this.signature = signature;
            // TODO: position the cursor on argument 0? Or not necessary?
            //dataSource.nextValue(); // TODO handling of the event?
            firstArgumentStart = (int) dataSource.valueMarker.startIndex;
            firstArgumentEnd = (int) dataSource.valueMarker.endIndex;
            firstArgumentTypeId = dataSource.valueMarker.typeId;
            currentArgumentIndex = -1;
            return this;
        }

        @Override
        public IonReaderContinuableCore evaluateArgument(int argumentIndex, String fieldName) {
            long presence = presenceBitmap.get(argumentIndex);
            if (presence == PresenceBitmap.VOID) {
                return voidCursor;
            }
            // Position the reader on the argument with the given index
            if (argumentIndex != currentArgumentIndex) {
                if (argumentIndex == currentArgumentIndex + 1) {
                    currentArgumentIndex += 1;
                } else {
                    dataSource.sliceAfterHeader(firstArgumentStart, firstArgumentEnd, firstArgumentTypeId);
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
                return new LimitedIonCursor().initialize(dataSource, signature.get(argumentIndex), false, fieldName);
            } else if (presence == PresenceBitmap.GROUP) {
                return new LimitedIonCursor().initialize(dataSource, signature.get(argumentIndex), true, fieldName);
            } else {
                throw new IonException("Illegal presence bits.");
            }
        }

        @Override
        public void close() {
            dataSource.stepOutOfEExpressionFrom(currentArgumentIndex + 1, signature, presenceBitmap);
        }
    }

    private class LimitedIonCursor extends DelegatingIonReaderContinuableCore {

        private IonCursorBinary delegate;
        private int startDepth;
        private boolean isExhausted;
        private boolean isExpressionGroup;
        private Macro.Parameter parameter;
        private String fieldName = null;

        LimitedIonCursor initialize(ParsingIonCursorBinary delegate, Macro.Parameter parameter, boolean isExpressionGroup, String fieldName) {
            this.delegate = delegate;
            setDelegate(delegate);
            startDepth = delegate.containerIndex;
            isExhausted = false;
            this.parameter = parameter;
            this.isExpressionGroup = isExpressionGroup;
            this.fieldName = fieldName;
            if (isExpressionGroup) {
                if (parameter.getType() == Macro.ParameterEncoding.Tagged) {
                    delegate.enterTaggedArgumentGroup();
                } else {
                    delegate.enterTaglessArgumentGroup(parameter.getType().taglessEncodingKind);
                }
            }
            return this;
        }


        @Override
        public Event nextValue() {
            if (isExhausted) {
                if (isExpressionGroup) {
                    delegate.exitArgumentGroup();
                }
                popStackFrame();
                return Event.NEEDS_INSTRUCTION;
            }
            Event event;
            if (isExpressionGroup) {
                event = delegate.nextGroupedValue();
            } else {
                if (parameter.getType() == Macro.ParameterEncoding.Tagged) {
                    event = delegate.nextValue();
                } else {
                    event = delegate.nextTaglessValue(parameter.getType().taglessEncodingKind);
                }
            }
            if (event == Event.START_SCALAR) {
                if (delegate.containerIndex == startDepth) {
                    isExhausted = true;
                }
            }
            if (event == Event.NEEDS_INSTRUCTION) {
                if (delegate.valueTid != null && delegate.valueTid.isMacroInvocation) {
                    // TODO the delegate handles the evaluation... what is there to do here if anything?
                    if (delegate.containerIndex == startDepth) {
                        isExhausted = true;
                    }
                }
            }
            return event;
        }

        @Override
        public Event stepOutOfContainer() {
            Event event = delegate.stepOutOfContainer();
            // TODO this returns NEEDS_INSTRUCTION, which clashes with the exhausted condition. Needs fix
            if (delegate.containerIndex == startDepth) {
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

    private class MacroAwareIonCursorBinary extends DelegatingIonReaderContinuableCore {

        private ParsingIonCursorBinary delegate;

        MacroAwareIonCursorBinary initialize(ParsingIonCursorBinary delegate) {
            this.delegate = delegate;
            setDelegate(delegate);
            return this;
        }

        private Macro loadMacro() {
            long macroAddress = delegate.getMacroInvocationId();
            boolean isSystemMacro = delegate.isSystemInvocation();
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
            Event event = delegate.nextValue();
            if (event == Event.NEEDS_INSTRUCTION) {
                if (delegate.valueTid != null && delegate.valueTid.isMacroInvocation) {
                    Macro macro = loadMacro();
                    Bytecode bodyBytecode = macro.getBodyBytecode();
                    delegate.stepIntoEExpression();
                    PresenceBitmap presenceBitmap = delegate.loadPresenceBitmap(macro.getSignature(), presenceBitmapPool);
                    pushStackFrame(
                        // TODO when should parentArguments be non-null?
                        new BytecodeCursor().initialize(bodyBytecode, 0, bodyBytecode.size(), true, null, null),
                        new LazyArgumentSource().initialize(presenceBitmap, delegate, macro.getSignature())
                    );
                }
            }
            return event;
        }

        @Override
        public void close() throws IOException {
            // Do nothing
        }
    }

    private class BytecodeCursor implements IonReaderContinuableCore {

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

        private BytecodeArgumentSource argumentSource = new BytecodeArgumentSource();
        private ArgumentSource parentArguments;
        private Bytecode bytecode = null;
        private int programCounter;
        private int programCounterLimit;
        private boolean popStackWhenExhausted;

        public BytecodeCursor initialize(/*IonCursorBinary cursor, */Bytecode bytecode, int programCounterStart, int programCounterLimit, boolean popStackWhenExhausted, ArgumentSource parentArguments, String fieldName) {
            //this.cursor = cursor;
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
                        pushStackFrame(top.argumentSource.evaluateArgument(argumentIndex, fieldName), top.argumentSource);
                        return Event.NEEDS_INSTRUCTION;
                    case BytecodeOpcodes.OP_INVOKE_MACRO:
                        Macro macro = bytecode.getMacro(opcode & DATA_MASK);
                        Bytecode bodyBytecode = macro.getBodyBytecode();
                        pushStackFrame(
                            new BytecodeCursor().initialize(bodyBytecode, 0, bodyBytecode.size(), true, top.argumentSource, fieldName),
                            argumentSource
                        );
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
    }

    // The data source may either be an IonCursorBinary over the raw encoding or an interpreter over some bytecode
    private StackFrame pushStackFrame(IonReaderContinuableCore dataSource, ArgumentSource argumentSource) {
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
        stackFrame.argumentSource = argumentSource;
        stackFrame.dataSource = dataSource;
        top = stackFrame;
        setDelegate(top.dataSource);
        yield = false;
        return stackFrame;
    }

    private void popStackFrame() {
        if (--stackFrameIndex < 0) {
            top = null;
            return;
        }
        top = stack[stackFrameIndex];
        setDelegate(top.dataSource);
        yield = false;
    }

    @Override
    public IonCursor.Event nextValue() {
        Event event;
        do {
            yield = true;
            event = top.dataSource.nextValue();
        } while (!yield);
        return event;
    }

    @Override
    public void close() {
        popStackFrame();
    }
}
