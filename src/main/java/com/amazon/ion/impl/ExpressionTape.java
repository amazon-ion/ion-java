package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.PooledExpressionFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExpressionTape { // TODO make internal

    private final IonReaderContinuableCoreBinary reader;
    private final PooledExpressionFactory expressionPool = new PooledExpressionFactory(); // TODO remove
    private Object[] contexts = new Object[64];
    private ExpressionType[] types = new ExpressionType[64];
    private int[] starts = new int[64];
    private int[] ends = new int[64];
    private int i = 0;
    private int size = 0;

    ExpressionTape(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
    }

    void add(Object context, ExpressionType type, int start, int end) {
        if (i >= contexts.length) {
            contexts = Arrays.copyOf(contexts, contexts.length * 2);
            types = Arrays.copyOf(types, types.length * 2);
            starts = Arrays.copyOf(starts, starts.length * 2);
            ends = Arrays.copyOf(ends, ends.length * 2);
        }
        contexts[i] = context;
        types[i] = type;
        starts[i] = start;
        ends[i] = end;
        i++;
        size++;
    }

    public ExpressionType typeAt(int index) {
        return types[index];
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
}
