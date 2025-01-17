// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl.macro;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A factory for {@link Expression} instances. Avoids repetitive allocations by pooling instances. {@link #clear()}
 * returns all instances to the pool. Note: this should only be used when the lifetime of the provided instances
 * is known, and that lifetime must be the same for all instances provided between calls to {@link #clear()}.
 */
public class PooledExpressionFactory {

    private static final int POOL_SIZE = 32;

    private Expression.NullValue[] nullValues = new Expression.NullValue[POOL_SIZE];
    private Expression.BoolValue[] boolValues = new Expression.BoolValue[POOL_SIZE];
    private Expression.LongIntValue[] longIntValues = new Expression.LongIntValue[POOL_SIZE];
    private Expression.BigIntValue[] bigIntValues = new Expression.BigIntValue[POOL_SIZE];
    private Expression.FloatValue[] floatValues = new Expression.FloatValue[POOL_SIZE];
    private Expression.DecimalValue[] decimalValues = new Expression.DecimalValue[POOL_SIZE];
    private Expression.TimestampValue[] timestampValues = new Expression.TimestampValue[POOL_SIZE];
    private Expression.SymbolValue[] symbolValues = new Expression.SymbolValue[POOL_SIZE];
    private Expression.StringValue[] stringValues = new Expression.StringValue[POOL_SIZE];
    private Expression.ClobValue[] clobValues = new Expression.ClobValue[POOL_SIZE];
    private Expression.BlobValue[] blobValues = new Expression.BlobValue[POOL_SIZE];
    private Expression.FieldName[] fieldNames = new Expression.FieldName[POOL_SIZE];
    private Expression.EExpression[] eExpressions = new Expression.EExpression[POOL_SIZE];
    private Expression.ExpressionGroup[] expressionGroups = new Expression.ExpressionGroup[POOL_SIZE];
    private Expression.ListValue[] listValues = new Expression.ListValue[POOL_SIZE];
    private Expression.StructValue[] structValues = new Expression.StructValue[POOL_SIZE];
    private Expression.SExpValue[] sexpValues = new Expression.SExpValue[POOL_SIZE];

    private int nullValuesIndex = 0;
    private int boolValuesIndex = 0;
    private int longIntValuesIndex = 0;
    private int bigIntValuesIndex = 0;
    private int floatValuesIndex = 0;
    private int decimalValuesIndex = 0;
    private int timestampValuesIndex = 0;
    private int symbolValuesIndex = 0;
    private int stringValuesIndex = 0;
    private int clobValuesIndex = 0;
    private int blobValuesIndex = 0;
    private int fieldNamesIndex = 0;
    private int eExpressionsIndex = 0;
    private int expressionGroupsIndex = 0;
    private int listValuesIndex = 0;
    private int structValuesIndex = 0;
    private int sexpValuesIndex = 0;

    private <T> T[] grow(T[] array) {
        return Arrays.copyOf(array, array.length * 2);
    }

    public Expression.NullValue createNullValue(List<SymbolToken> annotations, IonType type) {
        Expression.NullValue expression;
        if (nullValuesIndex >= nullValues.length) {
            nullValues = grow(nullValues);
        }
        expression = nullValues[nullValuesIndex];
        if (expression == null) {
            expression = new Expression.NullValue(annotations, type);
            nullValues[nullValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setType(type);
        }
        nullValuesIndex++;
        return expression;
    }

    public Expression.BoolValue createBoolValue(List<SymbolToken> annotations, boolean value) {
        Expression.BoolValue expression;
        if (boolValuesIndex >= boolValues.length) {
            boolValues = grow(boolValues);
        }
        expression = boolValues[boolValuesIndex];
        if (expression == null) {
            expression = new Expression.BoolValue(annotations, value);
            boolValues[boolValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        boolValuesIndex++;
        return expression;
    }

    public Expression.LongIntValue createLongIntValue(List<SymbolToken> annotations, long value) {
        Expression.LongIntValue expression;
        if (longIntValuesIndex >= longIntValues.length) {
            longIntValues = grow(longIntValues);
        }
        expression = longIntValues[longIntValuesIndex];
        if (expression == null) {
            expression = new Expression.LongIntValue(annotations, value);
            longIntValues[longIntValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        longIntValuesIndex++;
        return expression;
    }

    public Expression.BigIntValue createBigIntValue(List<SymbolToken> annotations, BigInteger value) {
        Expression.BigIntValue expression;
        if (bigIntValuesIndex >= bigIntValues.length) {
            bigIntValues = grow(bigIntValues);
        }
        expression = bigIntValues[bigIntValuesIndex];
        if (expression == null) {
            expression = new Expression.BigIntValue(annotations, value);
            bigIntValues[bigIntValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        bigIntValuesIndex++;
        return expression;
    }


    public Expression.FloatValue createFloatValue(List<SymbolToken> annotations, double value) {
        Expression.FloatValue expression;
        if (floatValuesIndex >= floatValues.length) {
            floatValues = grow(floatValues);
        }
        expression = floatValues[floatValuesIndex];
        if (expression == null) {
            expression = new Expression.FloatValue(annotations, value);
            floatValues[floatValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        floatValuesIndex++;
        return expression;
    }

    public Expression.DecimalValue createDecimalValue(List<SymbolToken> annotations, BigDecimal value) {
        Expression.DecimalValue expression;
        if (decimalValuesIndex >= decimalValues.length) {
            decimalValues = grow(decimalValues);
        }
        expression = decimalValues[decimalValuesIndex];
        if (expression == null) {
            expression = new Expression.DecimalValue(annotations, value);
            decimalValues[decimalValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        decimalValuesIndex++;
        return expression;
    }

    public Expression.TimestampValue createTimestampValue(List<SymbolToken> annotations, Timestamp value) {
        Expression.TimestampValue expression;
        if (timestampValuesIndex >= timestampValues.length) {
            timestampValues = grow(timestampValues);
        }
        expression = timestampValues[timestampValuesIndex];
        if (expression == null) {
            expression = new Expression.TimestampValue(annotations, value);
            timestampValues[timestampValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        timestampValuesIndex++;
        return expression;
    }

    public Expression.SymbolValue createSymbolValue(List<SymbolToken> annotations, SymbolToken value) {
        Expression.SymbolValue expression;
        if (symbolValuesIndex >= symbolValues.length) {
            symbolValues = grow(symbolValues);
        }
        expression = symbolValues[symbolValuesIndex];
        if (expression == null) {
            expression = new Expression.SymbolValue(annotations, value);
            symbolValues[symbolValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        symbolValuesIndex++;
        return expression;
    }

    public Expression.StringValue createStringValue(List<SymbolToken> annotations, String value) {
        Expression.StringValue expression;
        if (stringValuesIndex >= stringValues.length) {
            stringValues = grow(stringValues);
        }
        expression = stringValues[stringValuesIndex];
        if (expression == null) {
            expression = new Expression.StringValue(annotations, value);
            stringValues[stringValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        stringValuesIndex++;
        return expression;
    }

    public Expression.ClobValue createClobValue(List<SymbolToken> annotations, byte[] value) {
        Expression.ClobValue expression;
        if (clobValuesIndex >= clobValues.length) {
            clobValues = grow(clobValues);
        }
        expression = clobValues[clobValuesIndex];
        if (expression == null) {
            expression = new Expression.ClobValue(annotations, value);
            clobValues[clobValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        clobValuesIndex++;
        return expression;
    }

    public Expression.BlobValue createBlobValue(List<SymbolToken> annotations, byte[] value) {
        Expression.BlobValue expression;
        if (blobValuesIndex >= blobValues.length) {
            blobValues = grow(blobValues);
        }
        expression = blobValues[blobValuesIndex];
        if (expression == null) {
            expression = new Expression.BlobValue(annotations, value);
            blobValues[blobValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setValue(value);
        }
        blobValuesIndex++;
        return expression;
    }

    public Expression.FieldName createFieldName(SymbolToken name) {
        Expression.FieldName expression;
        if (fieldNamesIndex >= fieldNames.length) {
            fieldNames = grow(fieldNames);
        }
        expression = fieldNames[fieldNamesIndex];
        if (expression == null) {
            expression = new Expression.FieldName(name);
            fieldNames[fieldNamesIndex] = expression;
        } else {
            expression.setValue(name);
        }
        fieldNamesIndex++;
        return expression;
    }

    public Expression.EExpression createEExpression(Macro macro, int selfIndex, int endExclusive) {
        Expression.EExpression expression;
        if (eExpressionsIndex >= eExpressions.length) {
            eExpressions = grow(eExpressions);
        }
        expression = eExpressions[eExpressionsIndex];
        if (expression == null) {
            expression = new Expression.EExpression(macro, selfIndex, endExclusive);
            eExpressions[eExpressionsIndex] = expression;
        } else {
            expression.setMacro(macro);
            expression.setSelfIndex(selfIndex);
            expression.setEndExclusive(endExclusive);
        }
        eExpressionsIndex++;
        return expression;
    }

    public Expression.ExpressionGroup createExpressionGroup(int selfIndex, int endExclusive) {
        Expression.ExpressionGroup expression;
        if (expressionGroupsIndex >= expressionGroups.length) {
            expressionGroups = grow(expressionGroups);
        }
        expression = expressionGroups[expressionGroupsIndex];
        if (expression == null) {
            expression = new Expression.ExpressionGroup(selfIndex, endExclusive);
            expressionGroups[expressionGroupsIndex] = expression;
        } else {
            expression.setSelfIndex(selfIndex);
            expression.setEndExclusive(endExclusive);
        }
        expressionGroupsIndex++;
        return expression;
    }

    public Expression.ListValue createListValue(List<SymbolToken> annotations, int selfIndex, int endExclusive) {
        Expression.ListValue expression;
        if (listValuesIndex >= listValues.length) {
            listValues = grow(listValues);
        }
        expression = listValues[listValuesIndex];
        if (expression == null) {
            expression = new Expression.ListValue(annotations, selfIndex, endExclusive);
            listValues[listValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setSelfIndex(selfIndex);
            expression.setEndExclusive(endExclusive);
        }
        listValuesIndex++;
        return expression;
    }

    public Expression.StructValue createStructValue(List<SymbolToken> annotations, int selfIndex, int endExclusive) {
        Expression.StructValue expression;
        if (structValuesIndex >= structValues.length) {
            structValues = grow(structValues);
        }
        expression = structValues[structValuesIndex];
        if (expression == null) {
            // TODO consider whether templateStructIndex could be leveraged or should be removed
            expression = new Expression.StructValue(annotations, selfIndex, endExclusive, Collections.emptyMap());
            structValues[structValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setSelfIndex(selfIndex);
            expression.setEndExclusive(endExclusive);
        }
        structValuesIndex++;
        return expression;
    }

    public Expression.SExpValue createSExpValue(List<SymbolToken> annotations, int selfIndex, int endExclusive) {
        Expression.SExpValue expression;
        if (sexpValuesIndex >= sexpValues.length) {
            sexpValues = grow(sexpValues);
        }
        expression = sexpValues[sexpValuesIndex];
        if (expression == null) {
            expression = new Expression.SExpValue(annotations, selfIndex, endExclusive);
            sexpValues[sexpValuesIndex] = expression;
        } else {
            expression.setAnnotations(annotations);
            expression.setSelfIndex(selfIndex);
            expression.setEndExclusive(endExclusive);
        }
        sexpValuesIndex++;
        return expression;
    }

    /**
     * Returns all instances to the pool.
     */
    public void clear() {
        nullValuesIndex = 0;
        boolValuesIndex = 0;
        longIntValuesIndex = 0;
        bigIntValuesIndex = 0;
        floatValuesIndex = 0;
        decimalValuesIndex = 0;
        timestampValuesIndex = 0;
        symbolValuesIndex = 0;
        stringValuesIndex = 0;
        clobValuesIndex = 0;
        blobValuesIndex = 0;
        fieldNamesIndex = 0;
        eExpressionsIndex = 0;
        expressionGroupsIndex = 0;
        listValuesIndex = 0;
        structValuesIndex = 0;
        sexpValuesIndex = 0;
    }
}
