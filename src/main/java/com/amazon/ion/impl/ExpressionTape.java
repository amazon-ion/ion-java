package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.OpCodes;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.SystemMacro;
import com.amazon.ion.impl.macro.TemplateMacro;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.amazon.ion.impl.ExpressionType.ANNOTATION_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_CONTAINER_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_SCALAR_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.EXPRESSION_GROUP_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.EXPRESSION_GROUP_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.E_EXPRESSION_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.E_EXPRESSION_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.VARIABLE_ORDINAL;

public class ExpressionTape { // TODO make internal

    public static class Element {
        Object context = null;
        Object value = null; // null for values that haven't yet been materialized
        byte type = -1;
        int start = -1;
        int end = -1;
        int containerStart = -1;
        int containerEnd = -1;
        SymbolToken fieldName = null;
    }

    public static class Core {
        private Element[] elements;
        private int[][] expressionStarts;
        private int size = 0;
        private int[] numberOfExpressions;
        private int numberOfVariables = 0;
        private int[] variableStarts;
        private int[] eExpressionStarts;
        private int[] eExpressionEnds;

        Core(int initialSize) {
            elements = new Element[initialSize];
            expressionStarts = new int[1][]; // TODO figure out why this breaks when the first dimension is increased, then increase it
            expressionStarts[0] = new int[16];
            numberOfExpressions = new int[1];
            variableStarts = new int[8];
            eExpressionStarts = new int[8];
            eExpressionEnds = new int[8];
        }

        void grow() {
            elements = Arrays.copyOf(elements, elements.length * 2);
        }

        void growExpressionStartsForEExpression(int eExpressionIndex) {
            int[] expressionStartsForEExpression = expressionStarts[eExpressionIndex];
            expressionStarts[eExpressionIndex] = Arrays.copyOf(expressionStartsForEExpression, expressionStartsForEExpression.length * 2);
        }

        void ensureEExpressionIndexAvailable(int eExpressionIndex) {
            if (expressionStarts.length <= eExpressionIndex) {
                expressionStarts = Arrays.copyOf(expressionStarts, expressionStarts.length + 1);
                expressionStarts[expressionStarts.length - 1] = new int[16];
                eExpressionStarts = Arrays.copyOf(eExpressionStarts, eExpressionStarts.length + 1);
                eExpressionEnds = Arrays.copyOf(eExpressionEnds, eExpressionEnds.length + 1);
                numberOfExpressions = Arrays.copyOf(numberOfExpressions, numberOfExpressions.length + 1);
            }
        }

        void setNextExpression(int eExpressionIndex, int index) {
            int numberOfExpressionsInEExpression = numberOfExpressions[eExpressionIndex]++;
            if (expressionStarts[eExpressionIndex].length < numberOfExpressionsInEExpression) {
                growExpressionStartsForEExpression(eExpressionIndex);
            }
            expressionStarts[eExpressionIndex][numberOfExpressionsInEExpression] = index;
        }

        void setNextVariable(int index) {
            if (variableStarts.length <= numberOfVariables) {
                variableStarts = Arrays.copyOf(variableStarts, variableStarts.length * 2);
            }
            variableStarts[numberOfVariables++] = index;
        }

        int findEndOfExpression(int startIndex) {
            int relativeDepth = 0;
            int i = startIndex;
            loop: while (i < size) {
                switch (elements[i].type) {
                    case ANNOTATION_ORDINAL:
                    case DATA_MODEL_SCALAR_ORDINAL:
                    case VARIABLE_ORDINAL:
                        if (relativeDepth == 0) {
                            if (i > startIndex) {
                                break loop;
                            }
                        }
                        break;
                    case E_EXPRESSION_ORDINAL:
                    case EXPRESSION_GROUP_ORDINAL:
                    case DATA_MODEL_CONTAINER_ORDINAL:
                        if (relativeDepth == 0) {
                            if (i > startIndex) {
                                break loop;
                            }
                        }
                        relativeDepth++;
                        break;
                    case EXPRESSION_GROUP_END_ORDINAL:
                    case DATA_MODEL_CONTAINER_END_ORDINAL:
                    case E_EXPRESSION_END_ORDINAL:
                        if (--relativeDepth < 0) {
                            break loop;
                        }
                        break;
                    default:
                        throw new IllegalStateException();
                }
                i++;
            }
            return i;
        }

        public int copyToVariable(int startIndex, int variableIndex, ExpressionTape other) {
            int endIndex = variableStarts[variableIndex];
            other.copyFromRange(this, startIndex, endIndex);
            return endIndex + 1;
        }

        public SymbolToken fieldNameForVariable(int variableIndex) {
            return elements[variableStarts[variableIndex]].fieldName;
        }

        public int getNumberOfVariables() {
            return numberOfVariables;
        }

        public boolean areVariablesOrdered() {
            int previousVariableOrdinal = -1;
            for (int i = 0; i < numberOfVariables; i++) {
                int currentVariableOrdinal = (int) elements[variableStarts[i]].context;
                if (currentVariableOrdinal < previousVariableOrdinal) {
                    return false;
                }
                previousVariableOrdinal = currentVariableOrdinal;
            }
            return true;
        }

        /**
         * Given a variable index, which refers to the order that variables are used in this expression tape, returns
         * the ordinal of that variable, i.e. the order in which that variable occurs in the invocation.
         * For example, the IfNone system macro may have the tape (VARIABLE(0) VARIABLE(1) VARIABLE(0)). The variable
         * indices for that tape are (0 1 2) and the variable ordinals are (0 1 0).
         * @param variableIndex a variable index, which the caller ensures is less than `numberOfVariables`.
         * @return the ordinal of the variable at the given index.
         */
        public int getVariableOrdinal(int variableIndex) {
            return (int) elements[variableStarts[variableIndex]].context;
        }

        public int size() {
            return size;
        }
    }

    public static final ExpressionTape EMPTY = new ExpressionTape(new Core(0));

    private Core core;
    private final IonReaderContinuableCoreBinary reader; // If null, then the values are materialized
    private int i = 0;
    private int iNext = 0;
    private int depth = 0;
    private int numberOfEExpressions = 0;
    private int[] eExpressionActiveAtDepth = new int[8];
    private int currentDataModelContainerStart = 0;

    public ExpressionTape(IonReaderContinuableCoreBinary reader, int initialSize) {
        this.reader = reader;
        core = new Core(initialSize);
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public ExpressionTape(Core core) {
        reader = null;
        this.core = core;
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public Core core() {
        return core;
    }

    public void reset(Core core) {
        this.core = core;
        rewindTo(0);
    }

    private void increaseDepth(boolean isEExpression) {
        depth++;
        if (depth >= eExpressionActiveAtDepth.length) {
            eExpressionActiveAtDepth = Arrays.copyOf(eExpressionActiveAtDepth, eExpressionActiveAtDepth.length * 2);
        }
        eExpressionActiveAtDepth[depth] = isEExpression ? numberOfEExpressions - 1 : -1;
    }

    private void decreaseDepth() {
        depth--;
        core.elements[i].containerStart = currentDataModelContainerStart;
    }

    private void setChildExpressionIndex() {
        if (eExpressionActiveAtDepth[depth] >= 0) {
            core.setNextExpression(eExpressionActiveAtDepth[depth], i);
        }
    }

    private void setExpressionStart(byte type) {
        switch (type) {
            case E_EXPRESSION_ORDINAL:
                setChildExpressionIndex();
                core.eExpressionStarts[numberOfEExpressions] = i;
                core.elements[i].end = numberOfEExpressions++;
                core.ensureEExpressionIndexAvailable(numberOfEExpressions);
                core.elements[i].containerStart = currentDataModelContainerStart;
                increaseDepth(true);
                break;
            case DATA_MODEL_CONTAINER_ORDINAL:
                setChildExpressionIndex();
                core.elements[i].containerStart = currentDataModelContainerStart;
                currentDataModelContainerStart = i;
                increaseDepth(false);
                break;
            case EXPRESSION_GROUP_ORDINAL:
                setChildExpressionIndex();
                increaseDepth(false);
                break;
            case DATA_MODEL_SCALAR_ORDINAL:
            case ANNOTATION_ORDINAL:
                setChildExpressionIndex();
                core.elements[i].containerStart = currentDataModelContainerStart;
                break;
            case VARIABLE_ORDINAL:
                setChildExpressionIndex();
                core.setNextVariable(i);
                core.elements[i].containerStart = currentDataModelContainerStart;
                break;
            case E_EXPRESSION_END_ORDINAL:
                decreaseDepth();
                core.eExpressionEnds[eExpressionActiveAtDepth[depth + 1]] = i;
                break;
            case DATA_MODEL_CONTAINER_END_ORDINAL:
                decreaseDepth();
                core.elements[currentDataModelContainerStart].containerEnd = i;
                currentDataModelContainerStart = core.elements[currentDataModelContainerStart].containerStart;
                break;
            case EXPRESSION_GROUP_END_ORDINAL:
                decreaseDepth();
                break;
            default: break;
        }
    }

    void add(Object context, byte type, int start, int end, SymbolToken fieldName) {
        if (i >= core.elements.length) {
            core.grow();
        }
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        element.context = context;
        element.type = type;
        element.value = null;
        element.start = start;
        element.end = end;
        element.fieldName = fieldName;
        setExpressionStart(type);
        i++;
        core.size++;
    }

    private void inlineExpression(Core source, int sourceStart, int sourceEnd, Core arguments, int argumentsStart, SymbolToken fieldName) {
        int startIndex = core.size;
        for (int i = sourceStart; i < sourceEnd; i++) {
            Element sourceElement = source.elements[i];
            if (sourceElement.type == ExpressionType.VARIABLE_ORDINAL) {
                if (arguments == null) {
                    // This is a top-level template body. There is no invocation yet, so there is nothing to substitute.
                    copyFrom(source, i);
                    continue;
                }
                int variableIndex = (int) sourceElement.context;
                int eExpressionIndex = arguments.elements[argumentsStart].end;
                if (variableIndex >= arguments.numberOfExpressions[eExpressionIndex]) {
                    // This argument is elided.
                    if (core.elements[core.size - 1].context == SystemMacro.IfNone) {
                        // Elide the IfNone invocation, substitute the second argument, skip the third.
                        core.size--;
                        this.i--;
                        int expressionEnd = source.findEndOfExpression(++i);
                        // Note: keep the existing field name.
                        inlineExpression(source, i, expressionEnd, null, -1, fieldName());
                        i = sourceEnd;
                    } else {
                        // TODO does this ever occur in a position where it could simply be elided?
                        add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, null, sourceElement.fieldName);
                        add(null, ExpressionType.EXPRESSION_GROUP_END_ORDINAL, null, null);
                    }
                } else {
                    int expressionStart = arguments.expressionStarts[eExpressionIndex][variableIndex];
                    int expressionEnd = arguments.findEndOfExpression(expressionStart);
                    inlineExpression(arguments, expressionStart, expressionEnd, null, -1, sourceElement.fieldName);
                }
            } else if (sourceElement.type == ExpressionType.E_EXPRESSION_ORDINAL) {
                // Recursive inlining
                // TODO have a limit on depth / size
                Macro macro = (Macro) sourceElement.context;
                Core expressionSource = null;
                if (macro instanceof TemplateMacro) {
                    expressionSource = macro.getBodyTape();
                } else if (macro instanceof SystemMacro) {
                    SystemMacro systemMacro = (SystemMacro) macro;
                    // Some system macros cannot be inlined because they have special behavior that is not captured
                    // by a template body.
                    if (systemMacro.getBody() != null) {
                        expressionSource = systemMacro.getBodyTape();
                    }
                }
                if (expressionSource != null) {
                    inlineExpression(expressionSource, 0, expressionSource.size, source, i, sourceElement.fieldName);
                    i = source.findEndOfExpression(i) - 1; // TODO this passed with and without -1; missing coverage?
                } else {
                    // This invocation cannot be inlined.
                    // TODO de-duplicate with the branch below
                    int expressionEnd = source.findEndOfExpression(i);
                    copyFrom(source, i);
                    inlineExpression(source, i + 1, expressionEnd, arguments, argumentsStart, null);
                    i = expressionEnd - 1; // Note: the for loop increments i // TODO consider just using a while loop
                }
            } else if (ExpressionType.isContainerStart(sourceElement.type)) {
                int expressionEnd = source.findEndOfExpression(i);
                copyFrom(source, i);
                inlineExpression(source, i + 1, expressionEnd, arguments, argumentsStart, null);
                i = expressionEnd - 1; // Note: the for loop increments i // TODO consider just using a while loop
            } else {
                // Scalar or container end
                copyFrom(source, i);
            }
        }
        if (fieldName != null) {
            setFieldNameAt(startIndex, fieldName);
        }
    }

    private void add(Object context, byte type, Object value, SymbolToken fieldName) {
        if (i >= core.elements.length) {
            core.grow();
        }
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        element.context = context;
        element.type = type;
        element.value = value;
        element.start = -1;
        element.end = -1;
        element.fieldName = fieldName;
        setExpressionStart(type);
        i++;
        core.size++;
    }

    private void copyElement(Core other, int otherIndex) {
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        Element otherElement = other.elements[otherIndex];
        element.context = otherElement.context;
        element.type = otherElement.type;
        element.value = otherElement.value;
        element.start = otherElement.start;
        element.end = otherElement.end;
        element.fieldName = otherElement.fieldName;
        setExpressionStart(element.type);
    }

    void copyFromRange(Core other, int startIndex, int endIndex) {
        int copyLength = endIndex - startIndex;
        int destinationEnd = i + copyLength;
        while (destinationEnd >= core.elements.length) {
            core.grow();
        }
        for (int otherIndex = startIndex; otherIndex < endIndex; otherIndex++) {
            copyElement(other, otherIndex);
            i++;
        }
        core.size += copyLength;
    }

    private void copyFrom(Core other, int otherIndex) {
        if (i >= core.elements.length) {
            core.grow();
        }
        copyElement(other, otherIndex);
        i++;
        core.size++;
    }

    public void addScalar(IonType type, Object value, SymbolToken fieldName) {
        add(NON_NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR_ORDINAL, value, fieldName);
    }

    public void next() {
        i = iNext;
    }

    public void prepareNext() {
        iNext = i + 1;
    }

    public int currentIndex() {
        return i;
    }

    public byte type() {
        return core.elements[i].type;
    }

    public IonType ionType() {
        return ((IonTypeID) core.elements[i].context).type;
    }

    public Object context() {
        return core.elements[i].context;
    }

    public int size() {
        return core.size;
    }

    public void setFieldNameAt(int index, SymbolToken fieldName) {
        core.elements[index].fieldName = fieldName;
    }

    public void rewindTo(int index) { // TODO make this just rewind(), always called with 0
        i = index;
        iNext = index;
        depth = 0;
    }

    public boolean isExhausted() {
        return i >= core.size;
    }

    public void clear() {
        i = 0;
        iNext = 0;
        core.size = 0;
        Arrays.fill(core.numberOfExpressions, 0);
        Arrays.fill(eExpressionActiveAtDepth, -1);
        core.numberOfVariables = 0;
        depth = 0;
        numberOfEExpressions = 0;
    }

    public void setNextAfterEndOfEExpression(int eExpressionIndex) {
        iNext = core.eExpressionEnds[eExpressionIndex];
    }

    public void advanceToAfterEndEExpression(int eExpressionIndex) {
        i = core.eExpressionEnds[eExpressionIndex];
        iNext = i;
    }

    public void advanceToAfterEndContainer() {
        i = core.elements[core.elements[i].containerStart].containerEnd + 1;
        iNext = i;
    }

    void shiftIndicesLeft(int shiftAmount) {
        // TODO note: another way to handle this is to just store the shift amount and perform the subtraction on
        //  the way out. Would need to test that the stored shift amount applied only when necessary and determine
        //  whether multiple shifts could be applied to the same index (I think not)
        for (int i = 0; i < core.size; i++) {
            Element element = core.elements[i];
            if (element.type != ExpressionType.E_EXPRESSION_ORDINAL && element.start >= 0) {
                element.start -= shiftAmount;
                element.end -= shiftAmount;
            }
        }
    }

    private void prepareToRead(IonType expectedType) {
        Element element = core.elements[i];
        IonTypeID typeId = (IonTypeID) element.context;
        if (expectedType != typeId.type) {
            throw new IonException(String.format("Expected type %s, but found %s.", expectedType, typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
    }

    public List<SymbolToken> annotations() {
        return (List<SymbolToken>) core.elements[i].context;
    }

    public SymbolToken fieldName() {
        return core.elements[i].fieldName;
    }

    public boolean isNullValue() {
        IonTypeID typeId = (IonTypeID) core.elements[i].context;
        return typeId.isNull;
    }

    public boolean readBoolean() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (boolean) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.BOOL);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        boolean value = reader.booleanValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public long readLong() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return ((BigInteger) element.value).longValue();
            }
            return (long) element.value; // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        long value = reader.longValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigInteger readBigInteger() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return (BigInteger) element.value;
            }
            return BigInteger.valueOf((long) element.value); // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        BigInteger value = reader.bigIntegerValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public IntegerSize readIntegerSize() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return IntegerSize.BIG_INTEGER;
            }
            long longValue = (long) element.value;
            if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                return IntegerSize.INT;
            }
            return IntegerSize.LONG;
        }
        // TODO it's common to call getIntegerSize(), then the corresponding Value() method. The current implementation
        //  here involves a lot of duplicate work.
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        IntegerSize value = reader.getIntegerSize();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigDecimal readBigDecimal() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (BigDecimal) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (typeId.type != IonType.INT && typeId.type != IonType.DECIMAL) {
            throw new IonException(String.format("Expected int or decimal, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        Decimal value = reader.decimalValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public String readText() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof SymbolToken) {
                return ((SymbolToken) element.value).assumeText();
            }
            return (String) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        String value = reader.stringValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public SymbolToken readSymbol() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof SymbolToken) {
                return (SymbolToken) element.value;
            }
            return _Private_Utils.newSymbolToken((String) element.value);
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        SymbolToken value;
        if (typeId.type == IonType.SYMBOL) {
            value = reader.symbolValue();
        } else {
            value = _Private_Utils.newSymbolToken(reader.stringValue());
        }
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public int lobSize() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return ((byte[]) element.value).length;
        }
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        return element.end - element.start;
    }

    public byte[] readLob() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (byte[]) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        byte[] value = reader.newBytes();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public double readFloat() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (double) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.FLOAT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        double value = reader.doubleValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public Timestamp readTimestamp() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (Timestamp) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.TIMESTAMP);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        Timestamp value = reader.timestampValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    // TODO might be able to get around needing these by adding an ExpressionType value for NULL_SCALAR
    private static final IonTypeID[] NON_NULL_SCALAR_TYPE_IDS;
    private static final IonTypeID[] NULL_SCALAR_TYPE_IDS;

    static {
        IonType[] ionTypes = IonType.values();
        NON_NULL_SCALAR_TYPE_IDS = new IonTypeID[ionTypes.length];
        NULL_SCALAR_TYPE_IDS = new IonTypeID[ionTypes.length];
        for (IonType type : ionTypes) {
            int ordinal = type.ordinal();
            if (type == IonType.DATAGRAM) {
                continue;
            }
            if (type == IonType.NULL) {
                NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.NULL_UNTYPED & 0xFF];
                continue;
            }
            NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.NULL_TYPE_IDS_1_1[ordinal - 1];
            switch (type) {
                case BOOL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.BOOLEAN_TRUE & 0xFF];
                    break;
                case INT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.INTEGER_ZERO_LENGTH & 0xFF];
                    break;
                case FLOAT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.FLOAT_ZERO_LENGTH & 0xFF];
                    break;
                case DECIMAL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.DECIMAL_ZERO_LENGTH & 0xFF];
                    break;
                case TIMESTAMP:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.TIMESTAMP_DAY_PRECISION & 0xFF];
                    break;
                case SYMBOL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.SYMBOL_ADDRESS_1_BYTE & 0xFF];
                    break;
                case STRING:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.STRING_ZERO_LENGTH & 0xFF];
                    break;
                case CLOB:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.VARIABLE_LENGTH_CLOB & 0xFF];
                    break;
                case BLOB:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.VARIABLE_LENGTH_BLOB & 0xFF];
                    break;
                case LIST:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.LIST_ZERO_LENGTH & 0xFF];
                    break;
                case SEXP:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.SEXP_ZERO_LENGTH & 0xFF];
                    break;
                case STRUCT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.STRUCT_SID_ZERO_LENGTH & 0xFF];
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

    }

    public void addDataModelValue(Expression.DataModelValue value, SymbolToken fieldName) {
        List<SymbolToken> annotations = value.getAnnotations();
        if (!annotations.isEmpty()) {
            add(annotations, ExpressionType.ANNOTATION_ORDINAL, null, fieldName);
        }
        IonType type = value.getType();
        if (value instanceof Expression.NullValue) {
            add(NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR_ORDINAL, null, fieldName);
        } else {
            IonTypeID typeID = NON_NULL_SCALAR_TYPE_IDS[type.ordinal()];
            switch (type) {
                case BOOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BoolValue) value).getValue(), fieldName);
                    break;
                case INT:
                    if (value instanceof Expression.LongIntValue) {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.LongIntValue) value).getValue(), fieldName);
                    } else {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BigIntValue) value).getValue(), fieldName);
                    }
                    break;
                case FLOAT:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.FloatValue) value).getValue(), fieldName);
                    break;
                case DECIMAL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.DecimalValue) value).getValue(), fieldName);
                    break;
                case TIMESTAMP:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.TimestampValue) value).getValue(), fieldName);
                    break;
                case SYMBOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.SymbolValue) value).getValue(), fieldName);
                    break;
                case STRING:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.StringValue) value).getValue(), fieldName);
                    break;
                case CLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.ClobValue) value).getValue(), fieldName);
                    break;
                case BLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BlobValue) value).getValue(), fieldName);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static ExpressionTape.Core inlineNestedInvocations(ExpressionTape.Core tape) {
        ExpressionTape newTape = new ExpressionTape(null, tape.size + 64); // TODO arbitrary, adding some room in anticipation of inlining some invocations. Consider being more accurate
        newTape.inlineExpression(tape, 0, tape.size, null, -1, null);
        return newTape.core;
    }

    public static ExpressionTape.Core from(List<Expression> expressions) { // TODO possibly temporary, until Expression model is replaced
        // Note: this method doesn't really need to be efficient, as it happens (at most) once during compile time.
        // First pass: determine the required size of the tape. All container types require two tape expressions: one
        // start and one end. Everything else requires one tape expression.
        int tapeSize = 0;
        for (Expression expression : expressions) {
            tapeSize += (expression instanceof Expression.HasStartAndEnd) ? 2 : 1;
            if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = (Expression.DataModelValue) expression;
                if (!value.getAnnotations().isEmpty()) {
                    tapeSize++;
                }
            }
        }

        ExpressionTape tape = new ExpressionTape(null, tapeSize);
        List<Byte>[] ends = new List[expressions.size() + 1];
        SymbolToken fieldName = null;
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            List<Byte> endsAtExpressionIndex = ends[i];
            if (endsAtExpressionIndex != null) {
                for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                    tape.add(null, endsAtExpressionIndex.get(j), null, null);
                }
            }
            if (expression instanceof Expression.FieldName) {
                //tape.add(((Expression.FieldName) expression).getValue(), ExpressionType.FIELD_NAME_ORDINAL, null);
                fieldName = ((Expression.FieldName) expression).getValue();
                continue;
            } else if (expression instanceof Expression.EExpression) {
                Expression.EExpression eExpression = (Expression.EExpression) expression;
                tape.add(eExpression.getMacro(), ExpressionType.E_EXPRESSION_ORDINAL, null, fieldName);
                List<Byte> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END_ORDINAL);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.MacroInvocation) {
                Expression.MacroInvocation eExpression = (Expression.MacroInvocation) expression;
                tape.add(eExpression.getMacro(), ExpressionType.E_EXPRESSION_ORDINAL, null, fieldName);
                List<Byte> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END_ORDINAL);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.ExpressionGroup) {
                Expression.ExpressionGroup group = (Expression.ExpressionGroup) expression;
                tape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, null, fieldName); // TODO could pass along the endIndex as context?
                List<Byte> endsAtEndIndex = ends[group.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.EXPRESSION_GROUP_END_ORDINAL);
                ends[group.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.DataModelContainer) {
                Expression.DataModelContainer container = ((Expression.DataModelContainer) expression);
                List<SymbolToken> annotations = container.getAnnotations();
                if (!annotations.isEmpty()) {
                    tape.add(annotations, ExpressionType.ANNOTATION_ORDINAL, null, fieldName);
                }
                tape.add(NON_NULL_SCALAR_TYPE_IDS[container.getType().ordinal()], ExpressionType.DATA_MODEL_CONTAINER_ORDINAL, null, fieldName); // TODO could pass along the endIndex as context?
                if (container.getEndExclusive() == i) {
                    // This is an empty container
                    tape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL, null, fieldName);
                } else {
                    List<Byte> endsAtEndIndex = ends[container.getEndExclusive()];
                    if (endsAtEndIndex == null) {
                        endsAtEndIndex = new ArrayList<>();
                    }
                    endsAtEndIndex.add(ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL);
                    ends[container.getEndExclusive()] = endsAtEndIndex;
                }
            } else if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = ((Expression.DataModelValue) expression);
                tape.addDataModelValue(value, fieldName);
            } else if (expression instanceof Expression.VariableRef) {
                tape.add(((Expression.VariableRef) expression).getSignatureIndex(), ExpressionType.VARIABLE_ORDINAL, null, fieldName);
            } else {
                throw new IllegalStateException();
            }
            fieldName = null;
        }
        List<Byte> endsAtExpressionIndex = ends[expressions.size()];
        if (endsAtExpressionIndex != null) {
            for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                tape.add(null, endsAtExpressionIndex.get(j), null, null);
            }
        }
        tape.rewindTo(0);
        return tape.core;
    }

    public ExpressionTape seekToArgument(int eExpressionIndex, int indexRelativeToStart) {
        if (core.numberOfExpressions[eExpressionIndex] <= indexRelativeToStart) {
            return null;
        }
        i = core.expressionStarts[eExpressionIndex][indexRelativeToStart];
        iNext = i;
        return this;
    }

    /**
     * Transcodes the e-expression argument expressions provided to this MacroEvaluator
     * without evaluation.
     * @param writer the writer to which the expressions will be transcoded.
     */
    void transcodeArgumentsTo(MacroAwareIonWriter writer) throws IOException {
        int index = 0;
        rewindTo(0);
        while (index < size()) {
            next();
            prepareNext();
            byte argument = type();
            if (ExpressionType.isEnd(argument)) {
                writer.stepOut();
                index++;
                continue;
            }
            SymbolToken fieldName = fieldName();
            if (fieldName != null) {
                writer.setFieldNameSymbol(fieldName);
            }
            switch (argument) {
                case ExpressionType.ANNOTATION_ORDINAL:
                    writer.setTypeAnnotationSymbols(((List<SymbolToken>) context()).toArray(new SymbolToken[0]));
                    break;
                case ExpressionType.DATA_MODEL_CONTAINER_ORDINAL:
                    writer.stepIn(ionType());
                    break;
                case ExpressionType.DATA_MODEL_SCALAR_ORDINAL:
                    switch (ionType()) {
                        case NULL: writer.writeNull(); break;
                        case BOOL: writer.writeBool(readBoolean()); break;
                        case INT:
                            switch (readIntegerSize()) {
                                case INT: writer.writeInt(readLong()); break;
                                case LONG: writer.writeInt(readLong()); break;
                                case BIG_INTEGER: writer.writeInt(readBigInteger()); break;
                            }
                            break;
                        case FLOAT: writer.writeFloat(readFloat()); break;
                        case DECIMAL: writer.writeDecimal(readBigDecimal()); break;
                        case TIMESTAMP: writer.writeTimestamp(readTimestamp()); break;
                        case SYMBOL: writer.writeSymbolToken(readSymbol()); break;
                        case STRING: writer.writeString(readText()); break;
                        case BLOB: writer.writeBlob(readLob()); break;
                        case CLOB: writer.writeClob(readLob()); break;
                        default: throw new IllegalStateException("Unexpected branch");
                    }
                    break;
                case ExpressionType.E_EXPRESSION_ORDINAL:
                    writer.startMacro((Macro) context());
                    break;
                case ExpressionType.EXPRESSION_GROUP_ORDINAL:
                    writer.startExpressionGroup();
                    break;
                default: throw new IllegalStateException("Unexpected branch");
            }
            index++;
        }
        rewindTo(0);
    }
}
