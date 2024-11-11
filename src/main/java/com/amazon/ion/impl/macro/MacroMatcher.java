// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Matches source data to macro definitions.
 * TODO not supported yet: nested invocations
 */
public class MacroMatcher {

    private final TemplateMacro macro;
    private final String name;

    /**
     * Creates a matcher for the given TDL text.
     * @param macroText the TDL text that defines a single macro.
     * @param macroTable the macro table's mapping function.
     */
    public MacroMatcher(String macroText, Function<MacroRef, Macro> macroTable) {
        try (IonReader macroReader = IonReaderBuilder.standard().build(macroText)) {
            MacroCompiler compiler = new MacroCompiler(macroTable::apply, new ReaderAdapterIonReader(macroReader));
            macroReader.next();
            macro = compiler.compileMacro();
            name = compiler.getMacroName();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates a matcher for the macro on which the given reader is positioned.
     * @param macroReader the reader positioned on a TDL definition of a single macro.
     * @param macroTable the macro table's mapping function.
     */
    public MacroMatcher(IonReader macroReader, Function<MacroRef, Macro> macroTable) {
        MacroCompiler compiler = new MacroCompiler(macroTable::apply, new ReaderAdapterIonReader(macroReader));
        macro = compiler.compileMacro();
        name = compiler.getMacroName();
    }

    /**
     * @return the name of the macro.
     */
    public String name() {
        return name;
    }

    /**
     * @return the macro.
     */
    public TemplateMacro macro() {
        return macro;
    }

    private <T extends Expression> T requireExpressionType(Expression.TemplateBodyExpression expression, Class<T> requiredType) {
        if (requiredType.isAssignableFrom(expression.getClass())) {
            return requiredType.cast(expression);
        }
        return null;
    }

    /**
     * Attempts to match the value on which the reader is positioned to this matcher's macro by iterating over the value
     * and the macro body in lockstep until either an incompatibility is found (no match) or the value and body end
     * (match).
     * @param reader a reader positioned on a value to attempt to match to this matcher's macro.
     * @return true if the value matches this matcher's macro.
     */
    public boolean match(IonReader reader) {
        Iterator<Expression.TemplateBodyExpression> bodyIterator = macro.getBody().iterator();
        int index = 0;
        int[] numberOfContainerEndsAtExpressionIndex = new int[macro.getBody().size() + 1];
        while (true) {
            for (int i = 0; i < numberOfContainerEndsAtExpressionIndex[index]; i++) {
                if (reader.next() != null) {
                    return false;
                }
                reader.stepOut();
            }
            IonType type = reader.next();
            boolean hasNextExpression = bodyIterator.hasNext();
            Expression.TemplateBodyExpression expression = null;
            if (hasNextExpression) {
                expression = bodyIterator.next();
            } else if (type != null) {
                return false;
            }
            if (type == null) {
                if (expression instanceof  Expression.FieldName) {
                    expression = bodyIterator.next();
                }
                if (expression instanceof Expression.VariableRef) {
                    if (macro.getSignature().get(((Expression.VariableRef) expression).getSignatureIndex()).getCardinality().canBeVoid) {
                        // This is a trailing optional argument that is omitted in the source data, which is still
                        // considered compatible with the signature.
                        continue;
                    }
                    return false;
                } else if (hasNextExpression) {
                    return false;
                }
                break;
            }
            index++;
            if (expression instanceof Expression.FieldName) {
                if (!((Expression.FieldName) expression).getValue().assumeText().equals(reader.getFieldName())) {
                    return false;
                }
                if (!bodyIterator.hasNext()) {
                    throw new IllegalStateException("dangling field name");
                }
                expression = bodyIterator.next();
                index++;
            }
            if (expression instanceof Expression.VariableRef) {
                // For now, a variable matches any value at the current position.
                // TODO check cardinality and encoding type.
                continue;
            }
            if (expression instanceof Expression.ExpressionGroup) {
                throw new UnsupportedOperationException("TODO: handle expression groups");
            }
            if (expression instanceof Expression.MacroInvocation) {
                throw new UnsupportedOperationException("TODO: handle nested invocations");
            }
            if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue dataModelValueExpression = (Expression.DataModelValue) expression;
                if (!Arrays.asList(reader.getTypeAnnotationSymbols()).equals(dataModelValueExpression.getAnnotations())) {
                    return false;
                }
            }
            switch (type) {
                case NULL:
                    Expression.NullValue nullValue = requireExpressionType(expression, Expression.NullValue.class);
                    if (nullValue == null) {
                        return false;
                    }
                    break;
                case BOOL:
                    Expression.BoolValue boolValue = requireExpressionType(expression, Expression.BoolValue.class);
                    if (boolValue == null || (boolValue.getValue() != reader.booleanValue())) {
                        return false;
                    }
                    break;
                case INT:
                    switch (reader.getIntegerSize()) {
                        case INT:
                        case LONG:
                            Expression.LongIntValue intValue = requireExpressionType(expression, Expression.LongIntValue.class);
                            if (intValue == null || (intValue.getValue() != reader.longValue())) {
                                return false;
                            }
                            break;
                        case BIG_INTEGER:
                            Expression.BigIntValue bigIntValue = requireExpressionType(expression, Expression.BigIntValue.class);
                            if (bigIntValue == null || (!bigIntValue.getBigIntegerValue().equals(reader.bigIntegerValue()))) {
                                return false;
                            }
                            break;
                    }
                    break;
                case FLOAT:
                    Expression.FloatValue floatValue = requireExpressionType(expression, Expression.FloatValue.class);
                    if (floatValue == null || (Double.compare(floatValue.getValue(), reader.doubleValue()) != 0)) {
                        return false;
                    }
                    break;
                case DECIMAL:
                    Expression.DecimalValue decimalValue = requireExpressionType(expression, Expression.DecimalValue.class);
                    if (decimalValue == null || (!decimalValue.getValue().equals(reader.bigDecimalValue()))) {
                        return false;
                    }
                    break;
                case TIMESTAMP:
                    Expression.TimestampValue timestampValue = requireExpressionType(expression, Expression.TimestampValue.class);
                    if (timestampValue == null || (!timestampValue.getValue().equals(reader.timestampValue()))) {
                        return false;
                    }
                    break;
                case SYMBOL:
                    Expression.SymbolValue symbolValue = requireExpressionType(expression, Expression.SymbolValue.class);
                    if (symbolValue == null || (!symbolValue.getValue().assumeText().equals(reader.symbolValue().assumeText()))) {
                        return false;
                    }
                    break;
                case STRING:
                    Expression.StringValue stringValue = requireExpressionType(expression, Expression.StringValue.class);
                    if (stringValue == null || (!stringValue.getValue().equals(reader.stringValue()))) {
                        return false;
                    }
                    break;
                case CLOB:
                    Expression.ClobValue clobValue = requireExpressionType(expression, Expression.ClobValue.class);
                    if (clobValue == null || (!Arrays.equals(clobValue.getValue(), reader.newBytes()))) {
                        return false;
                    }
                    break;
                case BLOB:
                    Expression.BlobValue blobValue = requireExpressionType(expression, Expression.BlobValue.class);
                    if (blobValue == null || (!Arrays.equals(blobValue.getValue(), reader.newBytes()))) {
                        return false;
                    }
                    break;
                case LIST:
                    reader.stepIn();
                    Expression.ListValue listValue = requireExpressionType(expression, Expression.ListValue.class);
                    if (listValue == null) {
                        return false;
                    }
                    numberOfContainerEndsAtExpressionIndex[listValue.getEndExclusive()]++;
                    break;
                case SEXP:
                    reader.stepIn();
                    Expression.SExpValue sexpValue = requireExpressionType(expression, Expression.SExpValue.class);
                    if (sexpValue == null) {
                        return false;
                    }
                    numberOfContainerEndsAtExpressionIndex[sexpValue.getEndExclusive()]++;
                    break;
                case STRUCT:
                    reader.stepIn();
                    Expression.StructValue structValue = requireExpressionType(expression, Expression.StructValue.class);
                    if (structValue == null) {
                        return false;
                    }
                    numberOfContainerEndsAtExpressionIndex[structValue.getEndExclusive()]++;
                    break;
                case DATAGRAM:
                    throw new IllegalStateException();
            }
        }
        return true;
    }

    /**
     * @see #match(IonReader)
     * @param value the value to attempt to match.
     * @return true if the value matches this matcher's macro.
     */
    public boolean match(IonValue value) {
        try (IonReader domReader = IonReaderBuilder.standard().build(value)) {
            return match(domReader);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
