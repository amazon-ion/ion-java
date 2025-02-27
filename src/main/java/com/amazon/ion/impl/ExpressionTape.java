package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.PooledExpressionFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExpressionTape { // TODO make internal

    private final IonReaderContinuableCoreBinary reader; // If null, then the values are materialized
    private final PooledExpressionFactory expressionPool = new PooledExpressionFactory(); // TODO remove
    private Object[] contexts = new Object[64];
    private Object[] values = new Object[64]; // Is null for values that haven't yet been materialized
    private ExpressionType[] types = new ExpressionType[64];
    private int[] starts = new int[64];
    private int[] ends = new int[64];
    private int i = 0;
    private int size = 0;

    ExpressionTape(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
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

    void add(Object context, ExpressionType type, Object value) {
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

    public ExpressionType typeAt(int index) {
        return types[index];
    }

    public IonType ionTypeAt(int index) {
        switch (types[index]) {
            case DATA_MODEL_CONTAINER: return (IonType) contexts[index];
            case DATA_MODEL_SCALAR: return reader == null ? (IonType) contexts[index] : ((IonTypeID) contexts[index]).type;
            default: throw new IllegalStateException();
        }
    }

    public Object contextAt(int index) {
        return contexts[index];
    }

    public int size() {
        return size;
    }

    public void rewind() {
        i = 0;
    }

    void clear() {
        i = 0;
        size = 0;
        expressionPool.clear();
    }

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

    public int findIndexAfterEndEExpressionFrom(int index) {
        // TODO won't this be fooled by encountering a nested expression before the end?
        for (int i = index; i < size; i++) {
            if (types[i] == ExpressionType.E_EXPRESSION_END) {
                return i + 1;
            }
        }
        return index;
    }

    public int findIndexAfterEndContainerFrom(int index) {
        // TODO won't this be fooled by encountering a nested container before the end?
        for (int i = index; i < size; i++) {
            if (types[i].isEnd()) {
                return i + 1;
            }
        }
        return index;
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
                    switch (((IonType) context)) {
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

    private void prepareToReadAt(int tapeIndex, IonType expectedType) {
        /*
        if (tapeIndex >= size) {
            throw new IllegalStateException("Tape index " + tapeIndex + " is out of bounds for tape of size " + size);
        }

         */
        /*
        if (types[tapeIndex] != ExpressionType.DATA_MODEL_SCALAR) {
            throw new IllegalStateException("Tape index " + tapeIndex + " does not contain a data model scalar");
        }

         */
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (expectedType != typeId.type) {
            throw new IonException(String.format("Expected type %s, but found %s.", expectedType, typeId.type));
        }
        reader.sliceAfterHeader(starts[tapeIndex], ends[tapeIndex], typeId);
    }

    public List<SymbolToken> annotationsAt(int tapeIndex) {
        return (List<SymbolToken>) contexts[tapeIndex];
    }

    public boolean isNullValueAt(int tapeIndex) {
        if (reader == null) {
            return values[tapeIndex] == null;
        }
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        return typeId.isNull;
    }

    public boolean readBooleanAt(int tapeIndex) {
        if (reader == null) {
            return (boolean) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.BOOL);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        boolean value = reader.booleanValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public long readLongAt(int tapeIndex) {
        if (reader == null) {
            // TODO what about null ints? Handled externally?
            return (long) values[tapeIndex]; // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        long value = reader.longValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigInteger readBigIntegerAt(int tapeIndex) {
        if (reader == null) {
            return (BigInteger) values[tapeIndex]; // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        BigInteger value = reader.bigIntegerValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public IntegerSize readIntegerSizeAt(int tapeIndex) {
        if (reader == null) {
            if (values[tapeIndex] instanceof BigInteger) {
                return IntegerSize.BIG_INTEGER;
            }
            return IntegerSize.LONG; // TODO differentiate between int and long if necessary
        }
        // TODO it's common to call getIntegerSize(), then the corresponding Value() method. The current implementation
        //  here involves a lot of duplicate work.
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        IntegerSize value = reader.getIntegerSize();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigDecimal readBigDecimalAt(int tapeIndex) {
        if (reader == null) {
            return (BigDecimal) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (typeId.type != IonType.INT && typeId.type != IonType.DECIMAL) {
            throw new IonException(String.format("Expected int or decimal, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[tapeIndex], ends[tapeIndex], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        Decimal value = reader.decimalValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public String readTextAt(int tapeIndex) {
        if (reader == null) {
            if (contexts[tapeIndex] == IonType.SYMBOL) {
                return ((SymbolToken) values[tapeIndex]).assumeText();
            }
            return (String) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[tapeIndex], ends[tapeIndex], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        String value = reader.stringValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public SymbolToken readSymbolAt(int tapeIndex) {
        if (reader == null) {
            if (contexts[tapeIndex] == IonType.SYMBOL) {
                return (SymbolToken) values[tapeIndex];
            }
            return _Private_Utils.newSymbolToken((String) values[tapeIndex]);
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[tapeIndex], ends[tapeIndex], typeId);
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

    public int lobSize(int tapeIndex) {
        if (reader == null) {
            return ((byte[]) values[tapeIndex]).length;
        }
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        return ends[tapeIndex] - starts[tapeIndex];
    }

    public byte[] readLobAt(int tapeIndex) {
        if (reader == null) {
            return (byte[]) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) contexts[tapeIndex];
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(starts[tapeIndex], ends[tapeIndex], typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        byte[] value = reader.newBytes();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public double readFloatAt(int tapeIndex) {
        if (reader == null) {
            return (double) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.FLOAT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        double value = reader.doubleValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public Timestamp readTimestampAt(int tapeIndex) {
        if (reader == null) {
            return (Timestamp) values[tapeIndex];
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToReadAt(tapeIndex, IonType.TIMESTAMP);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value."); // TODO could have a nullable variant
        }
        Timestamp value = reader.timestampValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public Expression.EExpressionBodyExpression expressionAt(int tapeIndex) {
        List<SymbolToken> annotations = Collections.emptyList();
        while (true) {
            if (tapeIndex >= size) {
                return null;
            }
            int startIndex = starts[tapeIndex];
            Object context = contexts[tapeIndex];
            ExpressionType type = types[tapeIndex];
            int endIndex = ends[tapeIndex];
            Expression.EExpressionBodyExpression expression = null;
            switch (type) {
                case FIELD_NAME:
                    expression = expressionPool.createFieldName((SymbolToken) context);
                    break;
                case ANNOTATION:
                    annotations = (List<SymbolToken>) context;
                    tapeIndex++;
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
                    switch (((IonType) context)) {
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
            //tapeIndex++;
            if (expression != null) {
                if (expression instanceof Expression.DataModelValue) {
                    ((Expression.DataModelValue) expression).setAnnotations(annotations);
                }
            }
            return expression;
        }
    }

    public static ExpressionTape from(List<Expression> expressions) { // TODO possibly temporary, until Expression model is replaced
        // TODO need to inject END markers in the appropriate locations
        ExpressionTape tape = new ExpressionTape(null);
        for (Expression expression : expressions) {
            if (expression instanceof Expression.FieldName) {
                tape.add(((Expression.FieldName) expression).getValue(), ExpressionType.FIELD_NAME, null);
            } else if (expression instanceof Expression.EExpression) {
                tape.add(null, ExpressionType.E_EXPRESSION, ((Expression.EExpression) expression).getMacro());
            } else if (expression instanceof Expression.ExpressionGroup) {
                tape.add(null, ExpressionType.EXPRESSION_GROUP, null); // TODO could pass along the endIndex as context?
            } else if (expression instanceof Expression.DataModelContainer) {
                Expression.DataModelContainer container = ((Expression.DataModelContainer) expression);
                List<SymbolToken> annotations = container.getAnnotations();
                if (!annotations.isEmpty()) {
                    tape.add(annotations, ExpressionType.ANNOTATION, null);
                }
                tape.add(container.getType(), ExpressionType.DATA_MODEL_CONTAINER, null); // TODO could pass along the endIndex as context?
            } else if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = ((Expression.DataModelValue) expression);
                List<SymbolToken> annotations = value.getAnnotations();
                if (!annotations.isEmpty()) {
                    tape.add(null, ExpressionType.ANNOTATION, annotations);
                }
                IonType type = value.getType();
                if (expression instanceof Expression.NullValue) {
                    tape.add(type, ExpressionType.DATA_MODEL_SCALAR, null);
                } else {
                    switch (type) {
                    /*case NULL:
                        tape.add(IonType.NULL, ExpressionType.DATA_MODEL_SCALAR, null);
                        break;*/
                        case BOOL:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BoolValue) value).getValue());
                            break;
                        case INT:
                            if (expression instanceof Expression.LongIntValue) {
                                tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.LongIntValue) value).getValue());
                            } else {
                                tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BigIntValue) value).getValue());
                            }
                            break;
                        case FLOAT:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.FloatValue) value).getValue());
                            break;
                        case DECIMAL:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.DecimalValue) value).getValue());
                            break;
                        case TIMESTAMP:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.TimestampValue) value).getValue());
                            break;
                        case SYMBOL:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.SymbolValue) value).getValue());
                            break;
                        case STRING:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.StringValue) value).getValue());
                            break;
                        case CLOB:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.ClobValue) value).getValue());
                            break;
                        case BLOB:
                            tape.add(type, ExpressionType.DATA_MODEL_SCALAR, ((Expression.BlobValue) value).getValue());
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }
            } else if (expression instanceof Expression.VariableRef) {
                tape.add(((Expression.VariableRef) expression).getSignatureIndex(), ExpressionType.VARIABLE, null);
            } else {
                throw new IllegalStateException();
            }
        }
        return tape;
    }
}
