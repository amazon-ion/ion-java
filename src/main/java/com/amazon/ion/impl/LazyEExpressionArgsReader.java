// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.bin.PresenceBitmap;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.MacroEvaluator;
import com.amazon.ion.impl.macro.PooledExpressionFactory;
import com.amazon.ion.impl.macro.ReaderAdapter;
import com.amazon.ion.impl.macro.ReaderAdapterContinuable;
import com.amazon.ion.impl.macro.ReaderAdapterIonReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An {@link LazyEExpressionArgsReader} reads an E-Expression from a {@link ReaderAdapter}, constructs
 * a list of {@link Expression}s representing the E-Expression and its arguments, and prepares a {@link MacroEvaluator}
 * to evaluate these expressions.
 * <p>
 * There are two sources of expressions. The template macro definitions, and the macro arguments.
 * The {@link MacroEvaluator} merges those.
 * <p>
 * The {@link Expression} model does not (yet) support lazily reading values, so for now, all macro arguments must
 * be read eagerly.
 */
// TODO this isn't necessarily only related to e-expression args... this could be used generally to create a lazy tape
//  for any Ion value. This might be the most efficient way to "fill" Ion 1.1 delimited containers, as it reduces
//  the amount of redundant processing required in that case. The structural information learned during the fill
//  does not need to be rediscovered during the parse phase (if any).
abstract class LazyEExpressionArgsReader {

    enum ExpressionType {
        FIELD_NAME,
        ANNOTATION,
        E_EXPRESSION, // TODO might need a way to denote delimited vs prefixed logical containers, once we make it possible to not add the contents of prefixed containers to the tape
        E_EXPRESSION_END,
        EXPRESSION_GROUP,
        EXPRESSION_GROUP_END,
        DATA_MODEL_SCALAR,
        DATA_MODEL_CONTAINER,
        DATA_MODEL_CONTAINER_END,
    }

    static class ExpressionTape {

        private final PooledExpressionFactory expressionPool = new PooledExpressionFactory(); // TODO remove
        private Object[] contexts = new Object[64];
        private ExpressionType[] types = new ExpressionType[64];
        private int[] starts = new int[64];
        private int[] ends = new int[64];
        private int i = 0;
        private int size = 0;

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

        void rewind() {
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
        Expression.EExpressionBodyExpression dequeue(IonReaderContinuableCoreBinary reader, List<Expression.EExpressionBodyExpression> expressions) {
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
    }

    private final IonReaderContinuableCoreBinary reader;

    // Reusable sink for expressions.
    protected final List<Expression.EExpressionBodyExpression> expressions = new ArrayList<>(128);

    // Reusable tape for recording value boundaries for lazy parsing.
    protected final ExpressionTape expressionTape = new ExpressionTape();

    // Pool for presence bitmap instances.
    protected final PresenceBitmap.Companion.PooledFactory presenceBitmapPool = new PresenceBitmap.Companion.PooledFactory();

    /**
     * Constructor.
     * @param reader the {@link ReaderAdapter} from which to read {@link Expression}s.
     * @see ReaderAdapterIonReader
     * @see ReaderAdapterContinuable
     */
    LazyEExpressionArgsReader(IonReaderContinuableCoreBinary reader) {
        this.reader = reader;
        this.reader.setLeftShiftHandler(expressionTape::shiftIndicesLeft);
    }

    /**
     * @return true if the value upon which the reader is positioned represents a macro invocation; otherwise, false.
     */
    protected abstract boolean isMacroInvocation();

    /**
     * @return true if the container value on which the reader is positioned represents an expression group; otherwise,
     *  false.
     */
    protected abstract boolean isContainerAnExpressionGroup();

    /**
     * Eagerly collects the annotations on the current value.
     * @return the annotations, or an empty list if there are none.
     */
    protected abstract List<SymbolToken> getAnnotations();

    /**
     * Navigates the reader to the next raw value, without interpreting any system values.
     * @return true if there is a next value; false if the end of the container was reached.
     */
    protected abstract boolean nextRaw();

    /**
     * Steps into a container on which the reader has been positioned by calling {@link #nextRaw()}.
     */
    protected abstract void stepInRaw();

    /**
     * Steps out of a container on which the reader had been positioned by calling {@link #nextRaw()}.
     */
    protected abstract void stepOutRaw();

    /**
     * Steps into an e-expression.
     */
    protected abstract void stepIntoEExpression();

    /**
     * Steps out of an e-expression.
     */
    protected abstract void stepOutOfEExpression();

    /**
     * Reads a single parameter to a macro invocation.
     * @param parameter information about the parameter from the macro signature.
     * @param parameterPresence the presence bits dedicated to this parameter (unused in text).
     * @param isTrailing true if this parameter is the last one in the signature; otherwise, false (unused in binary).
     */
    protected abstract void readParameter(Macro.Parameter parameter, long parameterPresence, boolean isTrailing);

    /**
     * Reads the macro's address and attempts to resolve that address to a Macro from the macro table.
     * @return the loaded macro.
     */
    protected abstract Macro loadMacro();

    /**
     * Reads the argument encoding bitmap into a PresenceBitmap. This is only applicable to binary.
     * @param signature the macro signature.
     * @return a PresenceBitmap created from the argument encoding bitmap, or null.
     */
    protected abstract PresenceBitmap loadPresenceBitmapIfNecessary(List<Macro.Parameter> signature);

    /**
     * Reads a scalar value from the stream into an expression.
     */
    private void readScalarValueAsExpression() {
        expressionTape.add(reader.valueTid, ExpressionType.DATA_MODEL_SCALAR, (int) reader.valueMarker.startIndex, (int) reader.valueMarker.endIndex);
    }

    /**
     * Reads a container value from the stream into a list of expressions that will eventually be passed to
     * the MacroEvaluator responsible for evaluating the e-expression to which this container belongs.
     * @param type the type of container.
     */
    private void readContainerValueAsExpression(
        IonType type
    ) {
        boolean isExpressionGroup = isContainerAnExpressionGroup();
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP, -1, -1);
        } else {
            expressionTape.add(reader.valueTid.type, ExpressionType.DATA_MODEL_CONTAINER, -1, -1);
        }
        // TODO if the container is prefixed, don't recursively step through it
        stepInRaw();
        while (nextRaw()) {
            if (type == IonType.STRUCT) {
                // TODO avoid having to create SymbolToken every time
                expressionTape.add(reader.getFieldNameSymbol(), ExpressionType.FIELD_NAME, -1, -1);
            }
            readValueAsExpression(false); // TODO avoid recursion
        }
        if (isExpressionGroup) {
            expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END, -1, -1);
        } else {
            expressionTape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END, -1, -1);
        }
        stepOutRaw();
    }

    /**
     * Reads the rest of the stream into a single expression group.
     */
    private void readStreamAsExpressionGroup() {
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP, -1, -1);
        do {
            readValueAsExpression(false); // TODO avoid recursion
        } while (nextRaw());
        expressionTape.add(null, ExpressionType.EXPRESSION_GROUP_END, -1, -1);
    }

    /**
     * Reads a value from the stream into expression(s) that will eventually be passed to the MacroEvaluator
     * responsible for evaluating the e-expression to which this value belongs.
     * @param isImplicitRest true if this is the final parameter in the signature, it is variadic, and the format
     *                       supports implicit rest parameters (text only); otherwise, false.
     */
    protected void readValueAsExpression(boolean isImplicitRest) {
        if (isImplicitRest && !isContainerAnExpressionGroup()) {
            readStreamAsExpressionGroup();
            return;
        } else if (isMacroInvocation()) {
            collectEExpressionArgs(); // TODO avoid recursion
            return;
        }
        IonType type = reader.getEncodingType();
        if (reader.hasAnnotations()) {
            List<SymbolToken> annotations = getAnnotations(); // TODO make this lazy too
            expressionTape.add(annotations, ExpressionType.ANNOTATION, -1, -1);
        }
        if (IonType.isContainer(type) && !reader.isNullValue()) {
            readContainerValueAsExpression(type);
        } else {
            readScalarValueAsExpression();
        }
    }

    /**
     * Collects the expressions that compose the current macro invocation.
     */
    private void collectEExpressionArgs() {
        Macro macro = loadMacro();
        stepIntoEExpression();
        List<Macro.Parameter> signature = macro.getSignature();
        PresenceBitmap presenceBitmap = loadPresenceBitmapIfNecessary(signature);
        // TODO avoid eagerly stepping through prefixed expressions
        expressionTape.add(macro, ExpressionType.E_EXPRESSION, -1, -1);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                i == (numberOfParameters - 1)
            );
        }
        stepOutOfEExpression();
        expressionTape.add(null, ExpressionType.E_EXPRESSION_END, -1, -1);
    }

    // TODO step 1: modify this args reader to produce some sort of "tape" of non-materialized expressions (offsets/types)
    //  Right before calling initExpansion, materialize the tape into a list of expressions. This proves it can be done
    // TODO step 2: modify MacroEvaluator to operate on the "tape" instead of the materialized expressions. Remove
    //  the materialization

    private void materialize(IonReaderContinuableCoreBinary reader) {
        expressionTape.rewind();
        Expression.EExpressionBodyExpression expression;
        while ((expression = expressionTape.dequeue(reader, expressions)) != null) {
            expressions.add(expression);
        }
    }

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(MacroEvaluator macroEvaluator) {
        reader.pinBytesInCurrentValue();
        if (reader.isInStruct()) {
            // TODO avoid having to create SymbolToken every time
            expressionTape.add(reader.getFieldNameSymbol(), ExpressionType.FIELD_NAME, -1, -1);
        }
        collectEExpressionArgs();

        // TODO temporary: the MacroEvaluator should be modified to operate on the tape directly
        materialize(reader);
        macroEvaluator.initExpansion(expressions);
    }

    /**
     * Finishes evaluating the current macro invocation, resetting any associated state.
     */
    public void finishEvaluatingMacroInvocation() {
        expressions.clear();
        presenceBitmapPool.clear();
        expressionTape.clear();
        reader.unpinBytes();
        reader.returnToCheckpoint();
        expressionTape.clear();
    }
}
