package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.Ion_1_1_Constants;
import com.amazon.ion.impl.bin.OpCodes;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.PooledExpressionFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExpressionTape { // TODO make internal

    private final IonReaderContinuableCoreBinary reader; // If null, then the values are materialized
    private final PooledExpressionFactory expressionPool = new PooledExpressionFactory(); // TODO remove
    private boolean backedByReader;
    private Object[] contexts;
    private Object[] values; // Elements are null for values that haven't yet been materialized
    private ExpressionType[] types;
    private int[] starts;
    private int[] ends;
    private int i = 0;
    private int iNext = 0;
    private int size = 0;

    public ExpressionTape(IonReaderContinuableCoreBinary reader, int initialSize) {
        this.reader = reader;
        backedByReader = reader != null;
        contexts = new Object[initialSize];
        values = new Object[initialSize];
        types = new ExpressionType[initialSize];
        starts = new int[initialSize];
        ends = new int[initialSize];
    }

    private void grow() {
        contexts = Arrays.copyOf(contexts, contexts.length * 2);
        values = Arrays.copyOf(values, values.length * 2);
        types = Arrays.copyOf(types, types.length * 2);
        starts = Arrays.copyOf(starts, starts.length * 2);
        ends = Arrays.copyOf(ends, ends.length * 2);
    }

    void add(Object context, ExpressionType type, int start, int end) {
        if (i >= contexts.length) {
            grow();
        }
        contexts[i] = context;
        types[i] = type;
        values[i] = null;
        starts[i] = start;
        ends[i] = end;
        i++;
        size++;
    }

    private void add(Object context, ExpressionType type, Object value) {
        if (i >= contexts.length) {
            grow();
        }
        contexts[i] = context;
        types[i] = type;
        values[i] = value;
        starts[i] = -1;
        ends[i] = -1;
        i++;
        size++;
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
        return types[i];
    }

    public IonType ionType() {
        /*
        switch (types[i]) {
            case DATA_MODEL_CONTAINER: return (IonType) contexts[i];
            case DATA_MODEL_SCALAR: return ((IonTypeID) contexts[i]).type; //!backedByReader ? (IonType) contexts[i] : ((IonTypeID) contexts[i]).type;
            default: throw new IllegalStateException();
        }

         */
        return ((IonTypeID) contexts[i]).type;
    }

    public Object context() {
        return contexts[i];
    }

    public int size() {
        return size;
    }

    public void rewindTo(int index) {
        i = index;
        iNext = index;
    }

    public boolean isExhausted() {
        return i >= size;
    }

    public void clear() {
        i = 0;
        iNext = 0;
        size = 0;
        expressionPool.clear();
    }

    /*
    private void updateEndIndexFor(ExpressionType type, List<Expression.EExpressionBodyExpression> expressions) {
        // TODO note: this is not efficient, but it is temporary anyway. The evaluator will handle this using its
        //  evaluation stack.
        int end = expressions.size();
        for (int i = end - 1; i >= 0; i--) {
            Expression.EExpressionBodyExpression expression = expressions.get(i);
            switch (type) {
                case E_EXPRESSION:
                    if (expression instanceof Expression.EExpression) {
                        Expression.EExpression eexpression = ((Expression.EExpression) expression);
                        if (eexpression.getEndExclusive() < 0) {
                            eexpression.setEndExclusive(end);
                            return;
                        }
                    }
                    break;
                case EXPRESSION_GROUP:
                    if (expression instanceof Expression.ExpressionGroup) {
                        Expression.ExpressionGroup expressionGroup = ((Expression.ExpressionGroup) expression);
                        if (expressionGroup.getEndExclusive() < 0) {
                            expressionGroup.setEndExclusive(end);
                            return;
                        }
                    }
                    break;
                case DATA_MODEL_CONTAINER:
                    if (expression instanceof Expression.DataModelContainer) {
                        Expression.DataModelContainer container = ((Expression.DataModelContainer) expression);
                        if (container.getEndExclusive() < 0) {
                            container.setEndExclusive(end);
                            return;
                        }
                    }
                    break;
            }
        }
        throw new IllegalStateException("Unreachable: no start expression found for type " + type);
    }

     */

    public int findIndexAfterEndEExpression() {
        // TODO won't this be fooled by encountering a nested expression before the end?
        for (int index = i; index < size; index++) {
            if (types[index] == ExpressionType.E_EXPRESSION_END) {
                return index + 1;
            }
        }
        return i;
    }

    public void advanceToAfterEndEExpression() {
        // TODO won't this be fooled by encountering a nested expression before the end?
        while (i < size) {
            if (types[i++] == ExpressionType.E_EXPRESSION_END) {
                break;
            }
        }
        iNext = i;
    }

    public void advanceToAfterEndContainer() {
        // TODO won't this be fooled by encountering a nested expression before the end?
        while (i < size) {
            if (types[i++].isEnd()) {
                break;
            }
        }
        iNext = i;
    }

    void shiftIndicesLeft(int shiftAmount) {
        // TODO note: another way to handle this is to just store the shift amount and perform the subtraction on
        //  the way out. Would need to test that the stored shift amount applied only when necessary and determine
        //  whether multiple shifts could be applied to the same index (I think not)
        for (int i = 0; i < size; i++) {
            starts[i] -= shiftAmount;
            ends[i] -= shiftAmount;
        }
    }

    // TODO eventually, just prime the reader instead of returning an expression. The evaluator should operate
    //  on the reader directly to avoid materializing expressions.
    // TODO test early step-out or step-over before the tape is fully drained. The reader needs to be advanced to
    //  the end of the tape and prepared to continue reading.
/*
    public Expression.EExpressionBodyExpression dequeue(List<Expression.EExpressionBodyExpression> expressions) {
        List<SymbolToken> annotations = Collections.emptyList();
        while (true) {
            if (i >= size) {
                return null;
            }
            int startIndex = starts[i];
            Object context = contexts[i];
            ExpressionType type = types[i];
            int endIndex = ends[i];
            Expression.EExpressionBodyExpression expression = null;
            switch (type) {
                case FIELD_NAME:
                    expression = expressionPool.createFieldName((SymbolToken) context);
                    break;
                case ANNOTATION:
                    annotations = (List<SymbolToken>) context;
                    break;
                case E_EXPRESSION:
                    expression = expressionPool.createEExpression((Macro) context, expressions.size(), -1);
                    break;
                case E_EXPRESSION_END:
                    updateEndIndexFor(ExpressionType.E_EXPRESSION, expressions);
                    break;
                case EXPRESSION_GROUP:
                    expression = expressionPool.createExpressionGroup(expressions.size(), -1);
                    break;
                case EXPRESSION_GROUP_END:
                    updateEndIndexFor(ExpressionType.EXPRESSION_GROUP, expressions);
                    break;
                case DATA_MODEL_SCALAR:
                    boolean isEvaluating = reader.isEvaluatingEExpression;
                    reader.isEvaluatingEExpression = false; // TODO hack
                    reader.sliceAfterHeader(startIndex, endIndex, (IonTypeID) context); // TODO do this for containers too when prefixed instead of eagerly traversing them
                    if (reader.isNullValue()) {
                        expression = expressionPool.createNullValue(annotations, reader.getEncodingType());
                    } else {
                        switch (reader.getEncodingType()) {
                            case BOOL:
                                expression = expressionPool.createBoolValue(annotations, reader.booleanValue());
                                break;
                            case INT:
                                switch (reader.getIntegerSize()) {
                                    case INT:
                                    case LONG:
                                        expression = expressionPool.createLongIntValue(annotations, reader.longValue());
                                        break;
                                    case BIG_INTEGER:
                                        expression = expressionPool.createBigIntValue(annotations, reader.bigIntegerValue());
                                        break;
                                    default:
                                        throw new IllegalStateException();
                                }
                                break;
                            case FLOAT:
                                expression = expressionPool.createFloatValue(annotations, reader.doubleValue());
                                break;
                            case DECIMAL:
                                expression = expressionPool.createDecimalValue(annotations, reader.decimalValue());
                                break;
                            case TIMESTAMP:
                                expression = expressionPool.createTimestampValue(annotations, reader.timestampValue());
                                break;
                            case SYMBOL:
                                expression = expressionPool.createSymbolValue(annotations, reader.symbolValue());
                                break;
                            case STRING:
                                expression = expressionPool.createStringValue(annotations, reader.stringValue());
                                break;
                            case CLOB:
                                expression = expressionPool.createClobValue(annotations, reader.newBytes());
                                break;
                            case BLOB:
                                expression = expressionPool.createBlobValue(annotations, reader.newBytes());
                                break;
                            default:
                                throw new IllegalStateException();
                        }
                    }
                    reader.isEvaluatingEExpression = isEvaluating;
                    break;
                case DATA_MODEL_CONTAINER:
                    switch (((IonTypeID) context).type) {
                        case LIST:
                            expression = expressionPool.createListValue(annotations, expressions.size(), -1);
                            break;
                        case SEXP:
                            expression = expressionPool.createSExpValue(annotations, expressions.size(), -1);
                            break;
                        case STRUCT:
                            expression = expressionPool.createStructValue(annotations, expressions.size(), -1);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                case DATA_MODEL_CONTAINER_END:
                    updateEndIndexFor(ExpressionType.DATA_MODEL_CONTAINER, expressions);
                    break;
            }
            i++;
            if (expression != null) {
                if (expression instanceof Expression.DataModelValue) {
                    ((Expression.DataModelValue) expression).setAnnotations(annotations);
                }
                return expression;
            }
        }
    }

 */

    private void prepareToRead(IonType expectedType) {
        /*
        if (i >= size) {
            throw new IllegalStateException("Tape index " + i + " is out of bounds for tape of size " + size);
        }

         */
        /*
        if (types[i] != ExpressionType.DATA_MODEL_SCALAR) {
            throw new IllegalStateException("Tape index " + i + " does not contain a data model scalar");
        }

         */
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (expectedType != typeId.type) {
            throw new IonException(String.format("Expected type %s, but found %s.", expectedType, typeId.type));
        }
        reader.sliceAfterHeader(starts[i], ends[i], typeId);
    }

    public List<SymbolToken> annotations() {
        return (List<SymbolToken>) contexts[i];
    }

    public boolean isNullValue() {
        /*
        if (!backedByReader) {
            return values[i] == null;
        }

         */
        IonTypeID typeId = (IonTypeID) contexts[i];
        return typeId.isNull;
    }

    public boolean readBoolean() {
        if (!backedByReader) {
            return (boolean) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.BOOL);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        boolean value = reader.booleanValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public long readLong() {
        if (!backedByReader) {
            // TODO what about null ints? Handled externally?
            if (values[i] instanceof BigInteger) {
                return ((BigInteger) values[i]).longValue();
            }
            return (long) values[i]; // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        long value = reader.longValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigInteger readBigInteger() {
        if (!backedByReader) {
            if (values[i] instanceof BigInteger) {
                return (BigInteger) values[i];
            }
            return BigInteger.valueOf((long) values[i]); // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        BigInteger value = reader.bigIntegerValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public IntegerSize readIntegerSize() {
        if (!backedByReader) {
            if (values[i] instanceof BigInteger) {
                return IntegerSize.BIG_INTEGER;
            }
            long longValue = (long) values[i];
            if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                return IntegerSize.INT;
            }
            return IntegerSize.LONG; // TODO differentiate between int and long if necessary
        }
        // TODO it's common to call getIntegerSize(), then the corresponding Value() method. The current implementation
        //  here involves a lot of duplicate work.
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        IntegerSize value = reader.getIntegerSize();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigDecimal readBigDecimal() {
        if (!backedByReader) {
            return (BigDecimal) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (typeId.type != IonType.INT && typeId.type != IonType.DECIMAL) {
            throw new IonException(String.format("Expected int or decimal, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[i], ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        Decimal value = reader.decimalValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public String readText() {
        if (!backedByReader) {
            if (values[i] instanceof SymbolToken) {
                return ((SymbolToken) values[i]).assumeText();
            }
            return (String) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[i], ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        String value = reader.stringValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public SymbolToken readSymbol() {
        if (!backedByReader) {
            if (values[i] instanceof SymbolToken) {
                return (SymbolToken) values[i];
            }
            return _Private_Utils.newSymbolToken((String) values[i]);
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[i], ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
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
            return ((byte[]) values[i]).length;
        }
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        return ends[i] - starts[i];
    }

    public byte[] readLob() {
        if (!backedByReader) {
            return (byte[]) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[i];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[i], ends[i], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        byte[] value = reader.newBytes();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public double readFloat() {
        if (!backedByReader) {
            return (double) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.FLOAT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        double value = reader.doubleValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public Timestamp readTimestamp() {
        if (!backedByReader) {
            return (Timestamp) values[i];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.TIMESTAMP);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        Timestamp value = reader.timestampValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    /*
    public Expression.EExpressionBodyExpression expression() {
        List<SymbolToken> annotations = Collections.emptyList();
        while (true) {
            if (i >= size) {
                return null;
            }
            int startIndex = starts[i];
            Object context = contexts[i];
            ExpressionType type = types[i];
            int endIndex = ends[i];
            Expression.EExpressionBodyExpression expression = null;
            switch (type) {
                case FIELD_NAME:
                    expression = expressionPool.createFieldName((SymbolToken) context);
                    break;
                case ANNOTATION:
                    annotations = (List<SymbolToken>) context;
                    i++;
                    continue;
                case E_EXPRESSION:
                    expression = expressionPool.createEExpression((Macro) context, -1, -1);
                    break;
                case E_EXPRESSION_END:
                    //updateEndIndexFor(ExpressionType.E_EXPRESSION, expressions);
                    break;
                case EXPRESSION_GROUP:
                    //expression = expressionPool.createExpressionGroup(expressions.size(), -1);
                    break;
                case EXPRESSION_GROUP_END:
                    //updateEndIndexFor(ExpressionType.EXPRESSION_GROUP, expressions);
                    break;
                case DATA_MODEL_SCALAR:
                    boolean isEvaluating = reader.isEvaluatingEExpression;
                    reader.isEvaluatingEExpression = false; // TODO hack
                    reader.sliceAfterHeader(startIndex, endIndex, (IonTypeID) context); // TODO do this for containers too when prefixed instead of eagerly traversing them
                    if (reader.isNullValue()) {
                        expression = expressionPool.createNullValue(annotations, reader.getEncodingType());
                    } else {
                        switch (reader.getEncodingType()) {
                            case BOOL:
                                expression = expressionPool.createBoolValue(annotations, reader.booleanValue());
                                break;
                            case INT:
                                switch (reader.getIntegerSize()) {
                                    case INT:
                                    case LONG:
                                        expression = expressionPool.createLongIntValue(annotations, reader.longValue());
                                        break;
                                    case BIG_INTEGER:
                                        expression = expressionPool.createBigIntValue(annotations, reader.bigIntegerValue());
                                        break;
                                    default:
                                        throw new IllegalStateException();
                                }
                                break;
                            case FLOAT:
                                expression = expressionPool.createFloatValue(annotations, reader.doubleValue());
                                break;
                            case DECIMAL:
                                expression = expressionPool.createDecimalValue(annotations, reader.decimalValue());
                                break;
                            case TIMESTAMP:
                                expression = expressionPool.createTimestampValue(annotations, reader.timestampValue());
                                break;
                            case SYMBOL:
                                expression = expressionPool.createSymbolValue(annotations, reader.symbolValue());
                                break;
                            case STRING:
                                expression = expressionPool.createStringValue(annotations, reader.stringValue());
                                break;
                            case CLOB:
                                expression = expressionPool.createClobValue(annotations, reader.newBytes());
                                break;
                            case BLOB:
                                expression = expressionPool.createBlobValue(annotations, reader.newBytes());
                                break;
                            default:
                                throw new IllegalStateException();
                        }
                    }
                    reader.isEvaluatingEExpression = isEvaluating;
                    break;
                case DATA_MODEL_CONTAINER:
                    switch (((IonTypeID) context).type) {
                        case LIST:
                            expression = expressionPool.createListValue(annotations, -1, -1);
                            break;
                        case SEXP:
                            expression = expressionPool.createSExpValue(annotations, -1, -1);
                            break;
                        case STRUCT:
                            expression = expressionPool.createStructValue(annotations, -1, -1);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                    break;
                case DATA_MODEL_CONTAINER_END:
                    //updateEndIndexFor(ExpressionType.DATA_MODEL_CONTAINER, expressions);
                    break;
            }
            //i++;
            if (expression != null) {
                if (expression instanceof Expression.DataModelValue) {
                    ((Expression.DataModelValue) expression).setAnnotations(annotations);
                }
            }
            return expression;
        }
    }

     */

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

    public static ExpressionTape from(List<Expression> expressions) { // TODO possibly temporary, until Expression model is replaced
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
        //ExpressionType[] ends = new ExpressionType[expressions.size() + 1];
        List<ExpressionType>[] ends = new List[expressions.size() + 1];
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            /*
            while (tape.types[tape.i] != null) {
                if (!tape.types[tape.i].isEnd()) {
                    throw new IllegalStateException();
                }
                // An end marker has already been positioned at this location. Move to the next.
                tape.i++;
            }

             */
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
                //tape.add(eExpression.getEndExclusive(), null, ExpressionType.E_EXPRESSION_END, null);
                //ends[eExpression.getEndExclusive()] = ExpressionType.E_EXPRESSION_END;
                List<ExpressionType> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.MacroInvocation) {
                Expression.MacroInvocation eExpression = (Expression.MacroInvocation) expression;
                tape.add(eExpression.getMacro(), ExpressionType.E_EXPRESSION, null);
                //tape.add(eExpression.getEndExclusive(), null, ExpressionType.E_EXPRESSION_END, null);
                //ends[eExpression.getEndExclusive()] = ExpressionType.E_EXPRESSION_END;
                List<ExpressionType> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END);
                ends[eExpression.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.ExpressionGroup) {
                Expression.ExpressionGroup group = (Expression.ExpressionGroup) expression;
                tape.add(null, ExpressionType.EXPRESSION_GROUP, null); // TODO could pass along the endIndex as context?
                //tape.add(group.getEndExclusive(), null, ExpressionType.EXPRESSION_GROUP_END, null);
                //ends[group.getEndExclusive()] = ExpressionType.EXPRESSION_GROUP_END;
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
                //tape.add(container.getEndExclusive(), null, ExpressionType.DATA_MODEL_CONTAINER_END, null);
                //ends[container.getEndExclusive()] = ExpressionType.DATA_MODEL_CONTAINER_END;
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
        return tape;
    }

    public boolean seekToArgument(int startIndex, int indexRelativeToStart) {
        // TODO we probably should cache the arguments found so far to avoid iterating from the start for each arg
        // TODO OR, keep advancing the startIndex in the environment context. Go forward or backward
        int argIndex = 0;
        int relativeDepth = 0;
        i = startIndex;
        loop: while (i < size) { // TODO clean up
            switch (types[i]) {
                case FIELD_NAME:
                    // Skip the field name; advance to the following expression.
                    break;
                case ANNOTATION:
                case DATA_MODEL_SCALAR:
                    if (relativeDepth == 0) {
                        if (argIndex == indexRelativeToStart) {
                            break loop;
                        }
                        argIndex++;
                    }
                    break;
                case E_EXPRESSION:
                case EXPRESSION_GROUP:
                case DATA_MODEL_CONTAINER:
                    if (relativeDepth == 0) {
                        if (argIndex == indexRelativeToStart) {
                            break loop;
                        }
                        argIndex++;
                    }
                    relativeDepth++;
                    break;
                case EXPRESSION_GROUP_END:
                case DATA_MODEL_CONTAINER_END:
                case E_EXPRESSION_END:
                    if (--relativeDepth < 0) {
                        i++;
                        break loop;
                    }
                    break;
                default:
                    throw new IllegalStateException();
            }
            i++;
        }
        iNext = i;
        return argIndex >= indexRelativeToStart;
    }
}
