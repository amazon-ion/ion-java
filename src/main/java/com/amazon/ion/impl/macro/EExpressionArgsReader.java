// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.impl.bin.PresenceBitmap;

import java.util.ArrayList;
import java.util.List;

/**
 * An {@link EExpressionArgsReader} reads an E-Expression from a {@link ReaderAdapter}, constructs
 * a list of {@link Expression}s representing the E-Expression and its arguments, and prepares a {@link MacroEvaluator}
 * to evaluate these expressions.
 * <p>
 * There are two sources of expressions. The template macro definitions, and the macro arguments.
 * The {@link MacroEvaluator} merges those.
 * <p>
 * The {@link Expression} model does not (yet) support lazily reading values, so for now, all macro arguments must
 * be read eagerly.
 */
public abstract class EExpressionArgsReader {

    private final ReaderAdapter reader;

    // Reusable sink for expressions. The starting size of 64 is chosen so that growth is minimized or avoided for most
    // e-expression invocations.
    protected final List<Expression.EExpressionBodyExpression> expressions = new ArrayList<>(64);

    protected final PooledExpressionFactory expressionPool = new PooledExpressionFactory();

    /**
     * Constructor.
     * @param reader the {@link ReaderAdapter} from which to read {@link Expression}s.
     * @see ReaderAdapterIonReader
     * @see ReaderAdapterContinuable
     */
    public EExpressionArgsReader(ReaderAdapter reader) {
        this.reader = reader;
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
     * @param type the type of scalar.
     * @param annotations any annotations on the scalar.
     */
    private void readScalarValueAsExpression(
        IonType type,
        List<SymbolToken> annotations
    ) {
        Expression.EExpressionBodyExpression expression;
        if (reader.isNullValue()) {
            expression = expressionPool.createNullValue(annotations, type);
        } else {
            switch (type) {
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
        expressions.add(expression);
    }

    /**
     * Reads a container value from the stream into a list of expressions that will eventually be passed to
     * the MacroEvaluator responsible for evaluating the e-expression to which this container belongs.
     * @param type the type of container.
     * @param annotations any annotations on the container.
     */
    private void readContainerValueAsExpression(
        IonType type,
        List<SymbolToken> annotations
    ) {
        int startIndex = expressions.size();
        expressions.add(Expression.Placeholder.INSTANCE);
        boolean isExpressionGroup = isContainerAnExpressionGroup();
        // Eagerly parse the container, "compiling" it into expressions to be evaluated later.
        stepInRaw();
        while (nextRaw()) {
            if (type == IonType.STRUCT) {
                expressions.add(expressionPool.createFieldName(reader.getFieldNameSymbol()));
            }
            readValueAsExpression(false); // TODO avoid recursion
        }
        stepOutRaw();
        // Overwrite the placeholder with an expression representing the actual type of the container and the
        // start and end indices of its expressions.
        Expression.EExpressionBodyExpression expression;
        if (isExpressionGroup) {
            expression =  expressionPool.createExpressionGroup(startIndex, expressions.size());
        } else {
            switch (type) {
                case LIST:
                    expression = expressionPool.createListValue(annotations, startIndex, expressions.size());
                    break;
                case SEXP:
                    expression = expressionPool.createSExpValue(annotations, startIndex, expressions.size());
                    break;
                case STRUCT:
                    expression = expressionPool.createStructValue(annotations, startIndex, expressions.size());
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        expressions.set(startIndex, expression);
    }

    /**
     * Reads the rest of the stream into a single expression group.
     */
    private void readStreamAsExpressionGroup() {
        int startIndex = expressions.size();
        expressions.add(Expression.Placeholder.INSTANCE);
        do {
            readValueAsExpression(false); // TODO avoid recursion
        } while (nextRaw());
        expressions.set(startIndex, expressionPool.createExpressionGroup(startIndex, expressions.size()));
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
        IonType type = reader.encodingType();
        List<SymbolToken> annotations = getAnnotations();
        if (IonType.isContainer(type) && !reader.isNullValue()) {
            readContainerValueAsExpression(type, annotations);
        } else {
            readScalarValueAsExpression(type, annotations);
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
        int invocationStartIndex = expressions.size();
        expressions.add(Expression.Placeholder.INSTANCE);
        int numberOfParameters = signature.size();
        for (int i = 0; i < numberOfParameters; i++) {
            readParameter(
                signature.get(i),
                presenceBitmap == null ? PresenceBitmap.EXPRESSION : presenceBitmap.get(i),
                i == (numberOfParameters - 1)
            );
        }
        stepOutOfEExpression();
        expressions.set(invocationStartIndex, expressionPool.createEExpression(macro, invocationStartIndex, expressions.size()));
    }

    /**
     * Materializes the expressions that compose the macro invocation on which the reader is positioned and feeds
     * them to the macro evaluator.
     */
    public void beginEvaluatingMacroInvocation(MacroEvaluator macroEvaluator) {
        expressions.clear();
        expressionPool.clear();
        // TODO performance: avoid fully materializing all expressions up-front.
        if (reader.isInStruct()) {
            expressions.add(expressionPool.createFieldName(reader.getFieldNameSymbol()));
        }
        collectEExpressionArgs();
        macroEvaluator.initExpansion(expressions);
    }
}
