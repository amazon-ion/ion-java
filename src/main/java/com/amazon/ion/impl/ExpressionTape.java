package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.OpCodes;
import com.amazon.ion.impl.macro.Expression;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ExpressionTape { // TODO make internal

    public static class ExpressionPointer {
        ExpressionTape source = null;
        int index = -1;
        boolean isElided = false;

        public ExpressionTape visit() {
            source.i = index;
            source.iNext = index;
            return source;
        }

        public ExpressionTape visitIfForward() {
            if (index >= source.i) {
                source.i = index;
                source.iNext = index;
                return source;
            }
            return null;
        }
    }

    public static class Core {
        private Object[] contexts;
        private Object[] values; // Elements are null for values that haven't yet been materialized
        private ExpressionType[] types;
        private int[] starts;
        private int[] ends;
        private int[][] expressionStarts;
        private int size = 0;
        private int[] numberOfExpressions;
        private int numberOfVariables = 0;
        private int[] variableStarts;

        Core(int initialSize) {
            contexts = new Object[initialSize];
            values = new Object[initialSize];
            types = new ExpressionType[initialSize];
            starts = new int[initialSize];
            ends = new int[initialSize];
            expressionStarts = new int[1][];
            expressionStarts[0] = new int[16];
            numberOfExpressions = new int[1];
            variableStarts = new int[8];
        }

        void grow() {
            contexts = Arrays.copyOf(contexts, contexts.length * 2);
            values = Arrays.copyOf(values, values.length * 2);
            types = Arrays.copyOf(types, types.length * 2);
            starts = Arrays.copyOf(starts, starts.length * 2);
            ends = Arrays.copyOf(ends, ends.length * 2);
        }

        void growExpressionStartsForEExpression(int eExpressionIndex) {
            int[] expressionStartsForEExpression = expressionStarts[eExpressionIndex];
            expressionStarts[eExpressionIndex] = Arrays.copyOf(expressionStartsForEExpression, expressionStartsForEExpression.length * 2);
        }

        void ensureEExpressionIndexAvailable(int eExpressionIndex) {
            if (expressionStarts.length <= eExpressionIndex) {
                expressionStarts = Arrays.copyOf(expressionStarts, expressionStarts.length + 1);
                expressionStarts[expressionStarts.length - 1] = new int[16];
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
    }

    private Core core;
    private final IonReaderContinuableCoreBinary reader; // If null, then the values are materialized
    private boolean backedByReader;
    private int i = 0;
    private int iNext = 0;
    private int depth = 0;
    private int numberOfEExpressions = 0;
    private int[] eExpressionActiveAtDepth = new int[8]; // TODO consider generalizing this to a container type stack, and record container end indices for quick skip
    private ExpressionPointer[] expressionPointers = new ExpressionPointer[8];
    private int expressionPointersSize = 0;

    public ExpressionTape(IonReaderContinuableCoreBinary reader, int initialSize) {
        this.reader = reader;
        backedByReader = reader != null;
        core = new Core(initialSize);
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public ExpressionTape(Core core) {
        reader = null;
        backedByReader = false;
        this.core = core;
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public void reset(Core core) {
        this.core = core;
        expressionPointersSize = 0;
        rewindTo(0);
    }

    private void increaseDepth(boolean isEExpression) {
        depth++;
        if (depth >= eExpressionActiveAtDepth.length) {
            eExpressionActiveAtDepth = Arrays.copyOf(eExpressionActiveAtDepth, eExpressionActiveAtDepth.length * 2);
        }
        eExpressionActiveAtDepth[depth] = isEExpression ? numberOfEExpressions - 1 : -1;
    }

    private void setExpressionStart(ExpressionType type) {
        if (type == ExpressionType.E_EXPRESSION) {
            if (eExpressionActiveAtDepth[depth] >= 0) {
                core.setNextExpression(eExpressionActiveAtDepth[depth], i);
            }
            core.ends[i] = numberOfEExpressions++;
            core.ensureEExpressionIndexAvailable(numberOfEExpressions);
            increaseDepth(true);
        } else if (type.isContainerStart()) {
            if (eExpressionActiveAtDepth[depth] >= 0) {
                core.setNextExpression(eExpressionActiveAtDepth[depth], i);
            }
            increaseDepth(false);
        } else if (type == ExpressionType.DATA_MODEL_SCALAR || type == ExpressionType.ANNOTATION) { // TODO what about field names?
            if (eExpressionActiveAtDepth[depth] >= 0) {
                core.setNextExpression(eExpressionActiveAtDepth[depth], i);
            }
        } else if (type == ExpressionType.VARIABLE) {
            if (eExpressionActiveAtDepth[depth] >= 0) {
                core.setNextExpression(eExpressionActiveAtDepth[depth], i);
            }
            core.setNextVariable(i);
        } else if (type.isEnd()) {
            depth--;
        }
    }

    void add(Object context, ExpressionType type, int start, int end) {
        if (i >= core.contexts.length) {
            core.grow();
        }
        core.contexts[i] = context;
        core.types[i] = type;
        core.values[i] = null;
        core.starts[i] = start;
        core.ends[i] = end;
        setExpressionStart(type);
        i++;
        core.size++;
    }

    private void add(Object context, ExpressionType type, Object value) {
        if (i >= core.contexts.length) {
            core.grow();
        }
        core.contexts[i] = context;
        core.types[i] = type;
        core.values[i] = value;
        core.starts[i] = -1;
        core.ends[i] = -1;
        setExpressionStart(type);
        i++;
        core.size++;
    }

    public void addScalar(IonType type, Object value) {
        add(NON_NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR, value);
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

    public ExpressionType type() {
        return core.types[i];
    }

    public IonType ionType() {
        return ((IonTypeID) core.contexts[i]).type;
    }

    public Object context() {
        return core.contexts[i];
    }

    public int size() {
        return core.size;
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
        expressionPointersSize = 0;
    }

    public int findIndexAfterEndEExpression() {
        int relativeDepth = 0;
        for (int index = i; index < core.size; index++) {
            ExpressionType type = core.types[index];
            if (type == ExpressionType.E_EXPRESSION) {
                relativeDepth++;
            } else if (type == ExpressionType.E_EXPRESSION_END) {
                if (relativeDepth == 0) {
                    return index + 1;
                }
                relativeDepth--;
            }
        }
        return i;
    }

    public void setNextAfterEndOfEExpression() {
        int iCurrent = i;
        iNext = findIndexAfterEndEExpression();
        i = iCurrent;
    }

    // TODO deduplicate the following methods
    public void advanceToAfterEndEExpression() {
        int relativeDepth = 0;
        while (i < core.size) {
            ExpressionType type = core.types[i++];
            if (type == ExpressionType.E_EXPRESSION) {
                relativeDepth++;
            } else if (type == ExpressionType.E_EXPRESSION_END) {
                if (relativeDepth == 0) {
                    break;
                }
                relativeDepth--;
            }
        }
        iNext = i;
    }

    public void advanceToAfterEndContainer() {
        int relativeDepth = 0;
        while (i < core.size) {
            ExpressionType type = core.types[i++];
            if (type == ExpressionType.DATA_MODEL_CONTAINER) {
                relativeDepth++;
            } else if (type == ExpressionType.DATA_MODEL_CONTAINER_END) {
                if (relativeDepth == 0) {
                    break;
                }
                relativeDepth--;
            }
        }
        iNext = i;
    }

    void shiftIndicesLeft(int shiftAmount) {
        // TODO note: another way to handle this is to just store the shift amount and perform the subtraction on
        //  the way out. Would need to test that the stored shift amount applied only when necessary and determine
        //  whether multiple shifts could be applied to the same index (I think not)
        for (int i = 0; i < core.size; i++) {
            if (core.types[i] != ExpressionType.E_EXPRESSION && core.starts[i] >= 0) {
                core.starts[i] -= shiftAmount;
                core.ends[i] -= shiftAmount;
            }
        }
    }

    private void prepareToRead(IonType expectedType) {
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (expectedType != typeId.type) {
            throw new IonException(String.format("Expected type %s, but found %s.", expectedType, typeId.type));
        }
        reader.sliceAfterHeader(core.starts[i], core.ends[i], typeId);
    }

    public List<SymbolToken> annotations() {
        return (List<SymbolToken>) core.contexts[i];
    }

    public boolean isNullValue() {
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        return typeId.isNull;
    }

    public boolean readBoolean() {
        if (!backedByReader) {
            return (boolean) core.values[i];
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
        if (!backedByReader) {
            if (core.values[i] instanceof BigInteger) {
                return ((BigInteger) core.values[i]).longValue();
            }
            return (long) core.values[i]; // TODO add a nicer error if something is wrong
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
        if (!backedByReader) {
            if (core.values[i] instanceof BigInteger) {
                return (BigInteger) core.values[i];
            }
            return BigInteger.valueOf((long) core.values[i]); // TODO add a nicer error if something is wrong
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
        if (!backedByReader) {
            if (core.values[i] instanceof BigInteger) {
                return IntegerSize.BIG_INTEGER;
            }
            long longValue = (long) core.values[i];
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
        if (!backedByReader) {
            return (BigDecimal) core.values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (typeId.type != IonType.INT && typeId.type != IonType.DECIMAL) {
            throw new IonException(String.format("Expected int or decimal, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(core.starts[i], core.ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        Decimal value = reader.decimalValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public String readText() {
        if (!backedByReader) {
            if (core.values[i] instanceof SymbolToken) {
                return ((SymbolToken) core.values[i]).assumeText();
            }
            return (String) core.values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(core.starts[i], core.ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        String value = reader.stringValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public SymbolToken readSymbol() {
        if (!backedByReader) {
            if (core.values[i] instanceof SymbolToken) {
                return (SymbolToken) core.values[i];
            }
            return _Private_Utils.newSymbolToken((String) core.values[i]);
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(core.starts[i], core.ends[i], typeId);
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
        if (!backedByReader) {
            return ((byte[]) core.values[i]).length;
        }
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        return core.ends[i] - core.starts[i];
    }

    public byte[] readLob() {
        if (!backedByReader) {
            return (byte[]) core.values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) core.contexts[i];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(core.starts[i], core.ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        byte[] value = reader.newBytes();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public double readFloat() {
        if (!backedByReader) {
            return (double) core.values[i];
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
        if (!backedByReader) {
            return (Timestamp) core.values[i];
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

    public void addDataModelValue(Expression.DataModelValue value) {
        backedByReader = false;
        List<SymbolToken> annotations = value.getAnnotations();
        if (!annotations.isEmpty()) {
            add(annotations, ExpressionType.ANNOTATION, null);
        }
        IonType type = value.getType();
        if (value instanceof Expression.NullValue) {
            add(NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR, null);
        } else {
            IonTypeID typeID = NON_NULL_SCALAR_TYPE_IDS[type.ordinal()];
            switch (type) {
                case BOOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BoolValue) value).getValue());
                    break;
                case INT:
                    if (value instanceof Expression.LongIntValue) {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.LongIntValue) value).getValue());
                    } else {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BigIntValue) value).getValue());
                    }
                    break;
                case FLOAT:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.FloatValue) value).getValue());
                    break;
                case DECIMAL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.DecimalValue) value).getValue());
                    break;
                case TIMESTAMP:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.TimestampValue) value).getValue());
                    break;
                case SYMBOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.SymbolValue) value).getValue());
                    break;
                case STRING:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.StringValue) value).getValue());
                    break;
                case CLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.ClobValue) value).getValue());
                    break;
                case BLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BlobValue) value).getValue());
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
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
        List<ExpressionType>[] ends = new List[expressions.size() + 1];
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            List<ExpressionType> endsAtExpressionIndex = ends[i];
            if (endsAtExpressionIndex != null) {
                for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                    tape.add(null, endsAtExpressionIndex.get(j), null);
                }
            }
            if (expression instanceof Expression.FieldName) {
                tape.add(((Expression.FieldName) expression).getValue(), ExpressionType.FIELD_NAME, null);
            } else if (expression instanceof Expression.EExpression) {
                Expression.EExpression eExpression = (Expression.EExpression) expression;
                tape.add(eExpression.getMacro(), ExpressionType.E_EXPRESSION, null);
                List<ExpressionType> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.MacroInvocation) {
                Expression.MacroInvocation eExpression = (Expression.MacroInvocation) expression;
                tape.add(eExpression.getMacro(), ExpressionType.E_EXPRESSION, null);
                List<ExpressionType> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.ExpressionGroup) {
                Expression.ExpressionGroup group = (Expression.ExpressionGroup) expression;
                tape.add(null, ExpressionType.EXPRESSION_GROUP, null); // TODO could pass along the endIndex as context?
                List<ExpressionType> endsAtEndIndex = ends[group.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.EXPRESSION_GROUP_END);
                ends[group.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.DataModelContainer) {
                Expression.DataModelContainer container = ((Expression.DataModelContainer) expression);
                List<SymbolToken> annotations = container.getAnnotations();
                if (!annotations.isEmpty()) {
                    tape.add(annotations, ExpressionType.ANNOTATION, null);
                }
                tape.add(NON_NULL_SCALAR_TYPE_IDS[container.getType().ordinal()], ExpressionType.DATA_MODEL_CONTAINER, null); // TODO could pass along the endIndex as context?
                List<ExpressionType> endsAtEndIndex = ends[container.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.DATA_MODEL_CONTAINER_END);
                ends[container.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = ((Expression.DataModelValue) expression);
                tape.addDataModelValue(value);
            } else if (expression instanceof Expression.VariableRef) {
                tape.add(((Expression.VariableRef) expression).getSignatureIndex(), ExpressionType.VARIABLE, null);
            } else {
                throw new IllegalStateException();
            }
        }
        List<ExpressionType> endsAtExpressionIndex = ends[expressions.size()];
        if (endsAtExpressionIndex != null) {
            for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                tape.add(null, endsAtExpressionIndex.get(j), null);
            }
        }
        tape.rewindTo(0);
        return tape.core;
    }

    private ExpressionPointer addExpressionPointer() {
        if (expressionPointersSize <= expressionPointers.length) {
            expressionPointers = Arrays.copyOf(expressionPointers, expressionPointers.length * 2);
        }
        ExpressionPointer pointer = expressionPointers[expressionPointersSize++];
        if (pointer == null) {
            pointer = new ExpressionPointer();
            expressionPointers[expressionPointersSize - 1] = pointer;
        }
        return pointer;
    }

    private void addExpressionPointer(ExpressionTape source, int index) {
        ExpressionPointer pointer = addExpressionPointer();
        pointer.source = source;
        pointer.index = index;
        pointer.isElided = false;
    }

    private void addElidedExpressionPointer() {
        ExpressionPointer pointer = addExpressionPointer();
        pointer.source = null;
        pointer.index = -1;
        pointer.isElided = true;
    }

    public void cacheExpressionPointers(ExpressionTape arguments, int argumentsEExpressionStartIndex) {

        Core argumentCore = arguments.core;
        int targetEExpressionIndex = argumentCore.ends[argumentsEExpressionStartIndex];
        int numberOfExpressions = argumentCore.numberOfExpressions[targetEExpressionIndex];
        if (numberOfExpressions == 0) {
            return;
        }
        for (int i = 0; i < numberOfExpressions; i++) {
            int expressionIndex = argumentCore.expressionStarts[targetEExpressionIndex][i];
            ExpressionType targetType = argumentCore.types[expressionIndex];
            if (targetType == ExpressionType.VARIABLE) {
                int localVariableId = (int) argumentCore.contexts[expressionIndex];
                if (localVariableId >= arguments.expressionPointersSize) {
                    // The variable was elided.
                    addElidedExpressionPointer();
                } else {
                    ExpressionPointer pointer = arguments.expressionPointers[localVariableId];
                    int pointerIndex = pointer.index;
                    ExpressionTape pointerSource = pointer.source;
                    pointer.index = -1; // Mark this as a pass-through variable
                    if (pointerIndex < 0) {
                        // This localVariableId must have already been resolved, so it's somewhere in the list already.
                        for (int j = 0; j < i; j++) {
                            int previousExpressionIndex = argumentCore.expressionStarts[targetEExpressionIndex][i];
                            ExpressionType previousTargetType = argumentCore.types[expressionIndex];
                            if (previousTargetType == ExpressionType.VARIABLE && localVariableId == (int) argumentCore.contexts[previousExpressionIndex]) {
                                pointer = expressionPointers[j];
                                pointerSource = pointer.source;
                                pointerIndex = pointer.index;
                                break;
                            }
                        }
                    }
                    addExpressionPointer(pointerSource, pointerIndex);
                }
            } else {
                addExpressionPointer(arguments, expressionIndex);
            }
        }
    }


    public void seekPastExpression() {
        // TODO we probably should cache the arguments found so far to avoid iterating from the start for each arg
        // TODO OR, keep advancing the startIndex in the environment context. Go forward or backward
        int relativeDepth = 0;
        int startIndex = i;
        loop: while (i < core.size) {
            switch (core.types[i]) {
                case FIELD_NAME:
                case ANNOTATION:
                case DATA_MODEL_SCALAR:
                case VARIABLE:
                    if (relativeDepth == 0) {
                        if (i > startIndex) {
                            break loop;
                        }
                    }
                    break;
                case E_EXPRESSION:
                case EXPRESSION_GROUP:
                case DATA_MODEL_CONTAINER:
                    if (relativeDepth == 0) {
                        if (i > startIndex) {
                            break loop;
                        }
                    }
                    relativeDepth++;
                    break;
                case EXPRESSION_GROUP_END:
                case DATA_MODEL_CONTAINER_END:
                case E_EXPRESSION_END:
                    if (--relativeDepth < 0) {
                        break loop;
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
            i++;
        }
        i++; // This positions the tape *after* the end of the expression that was located above.
        iNext = i;
    }

    public ExpressionTape seekToArgument(int indexRelativeToStart) {
        ExpressionPointer pointer = expressionPointers[indexRelativeToStart];
        if (pointer.index < 0) { // TODO performance this might be able to be short-circuited more effectively. Right now we still create a child expander for elided variables, which should not be necessary.
            return null;
        }
        return pointer.visit();
    }

    public void seekPastFinalArgument() {
        for (int i = 0; i < expressionPointersSize; i++) {
            ExpressionPointer variablePointer = expressionPointers[i];
            if (variablePointer.isElided) {
                continue;
            } else if (variablePointer.index < 0) {
                if (variablePointer.source != null) {
                    // This is a pass-through variable; step over it if that hasn't been done already.
                    variablePointer.source.iNext = Math.max(variablePointer.source.i + 1, variablePointer.source.iNext);
                }
                continue;
            }
            ExpressionTape sourceTape = variablePointer.visitIfForward();
            if (sourceTape != null) {
                sourceTape.seekPastExpression();
            }
        }
    }
}
