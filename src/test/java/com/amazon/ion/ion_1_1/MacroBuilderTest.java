// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.ion_1_1;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import com.amazon.ion.bytecode.ir.OperationKind;
import com.amazon.ion.bytecode.util.ByteSlice;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/// Test class for MacroBuilder to verify it produces valid Macro instances
/// and provides a fluent, Java-friendly API.
///
/// Your IDE may complain about OperationKind and ByteSlice because "Usage of Kotlin internal declaration from different module"
/// This error is a false positive. The tests module _can_ access internal declarations from the main module.
public class MacroBuilderTest {

    @Test
    public void testNullValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .nullValue()
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check null value
        TemplateExpression nullExpr = expressions.get(0);
        assertEquals(OperationKind.NULL, nullExpr.expressionKind);
        assertEquals(IonType.NULL, nullExpr.objectValue);
    }

    @Test
    public void testBoolValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .boolValue(true)
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check boolean value
        TemplateExpression boolExpr = expressions.get(0);
        assertEquals(OperationKind.BOOL, boolExpr.expressionKind);
        assertEquals(1L, boolExpr.primitiveValue); // true = 1
    }

    @Test
    public void testIntValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .intValue(42L)
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check integer value
        TemplateExpression intExpr = expressions.get(0);
        assertEquals(OperationKind.INT, intExpr.expressionKind);
        assertEquals(42L, intExpr.primitiveValue);
    }

    @Test
    public void testFloatValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .floatValue(3.14)
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check float value
        TemplateExpression floatExpr = expressions.get(0);
        assertEquals(OperationKind.FLOAT, floatExpr.expressionKind);
        assertEquals(Double.doubleToRawLongBits(3.14), floatExpr.primitiveValue);
    }

    @Test
    public void testStringValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .stringValue("hello")
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check string value
        TemplateExpression stringExpr = expressions.get(0);
        assertEquals(OperationKind.STRING, stringExpr.expressionKind);
        assertEquals("hello", stringExpr.objectValue);
    }

    @Test
    public void testSymbolValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .symbolValue("world")
            .build();

        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check symbol value
        TemplateExpression symbolExpr = expressions.get(0);
        assertEquals(OperationKind.SYMBOL, symbolExpr.expressionKind);
        assertEquals("world", symbolExpr.objectValue);
    }

    @Test
    public void testBigIntegerValue() {
        BigInteger bigInt = new BigInteger("123456789012345678901234567890");
        
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .intValue(bigInt)
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check BigInteger value
        TemplateExpression intExpr = expressions.get(0);
        assertEquals(OperationKind.INT, intExpr.expressionKind);
        assertEquals(bigInt, intExpr.objectValue);
        assertEquals(0L, intExpr.primitiveValue); // BigInteger uses objectValue, not primitiveValue
    }

    @Test
    public void testBigDecimalValue() {
        BigDecimal bigDec = new BigDecimal("123.456");
        
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .decimalValue(bigDec)
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check BigDecimal value (converted to Decimal)
        TemplateExpression decExpr = expressions.get(0);
        assertEquals(OperationKind.DECIMAL, decExpr.expressionKind);
        assertTrue(decExpr.objectValue instanceof Decimal);
        assertEquals(Decimal.valueOf(bigDec), decExpr.objectValue);
    }

    @Test
    public void testTimestampValue() {
        Timestamp ts = Timestamp.valueOf("2025-01-01T");

        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .timestampValue(ts)
            .build();

        assertNotNull(macro);

        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());

        // Check BigDecimal value (converted to Decimal)
        TemplateExpression tsExpr = expressions.get(0);
        assertEquals(OperationKind.TIMESTAMP, tsExpr.expressionKind);
        assertTrue(tsExpr.objectValue instanceof Timestamp);
        assertEquals(ts, tsExpr.objectValue);
    }

    @Test
    public void testListValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .listValue(list -> list
                .stringValue("item1")
                .intValue(42)
                .boolValue(false))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check list container
        TemplateExpression listExpr = expressions.get(0);
        assertEquals(OperationKind.LIST, listExpr.expressionKind);
        assertEquals(3, listExpr.childValues.length);
        
        // Check list contents
        assertEquals(OperationKind.STRING, listExpr.childValues[0].expressionKind);
        assertEquals("item1", listExpr.childValues[0].objectValue);
        
        assertEquals(OperationKind.INT, listExpr.childValues[1].expressionKind);
        assertEquals(42L, listExpr.childValues[1].primitiveValue);
        
        assertEquals(OperationKind.BOOL, listExpr.childValues[2].expressionKind);
        assertEquals(0L, listExpr.childValues[2].primitiveValue); // false = 0
    }

    @Test
    public void testSexpValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .sexpValue(sexp -> sexp
                .symbolValue("add")
                .intValue(1)
                .intValue(2))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check S-expression container
        TemplateExpression sexpExpr = expressions.get(0);
        assertEquals(OperationKind.SEXP, sexpExpr.expressionKind);
        assertEquals(3, sexpExpr.childValues.length);
        
        // Check S-expression contents
        assertEquals(OperationKind.SYMBOL, sexpExpr.childValues[0].expressionKind);
        assertEquals("add", sexpExpr.childValues[0].objectValue);
        
        assertEquals(OperationKind.INT, sexpExpr.childValues[1].expressionKind);
        assertEquals(1L, sexpExpr.childValues[1].primitiveValue);
        
        assertEquals(OperationKind.INT, sexpExpr.childValues[2].expressionKind);
        assertEquals(2L, sexpExpr.childValues[2].primitiveValue);
    }

    @Test
    public void testStructValue() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .structValue(struct -> struct
                .fieldName("name")
                .stringValue("John")
                .fieldName("age")
                .intValue(30)
                .fieldName("active")
                .boolValue(true))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check struct container
        TemplateExpression structExpr = expressions.get(0);
        assertEquals(OperationKind.STRUCT, structExpr.expressionKind);
        assertEquals(6, structExpr.childValues.length); // 3 field names + 3 values
        
        // Check struct contents (field name + value pairs)
        assertEquals(OperationKind.FIELD_NAME, structExpr.childValues[0].expressionKind);
        assertEquals("name", structExpr.childValues[0].objectValue);
        
        assertEquals(OperationKind.STRING, structExpr.childValues[1].expressionKind);
        assertEquals("John", structExpr.childValues[1].objectValue);
        
        assertEquals(OperationKind.FIELD_NAME, structExpr.childValues[2].expressionKind);
        assertEquals("age", structExpr.childValues[2].objectValue);
        
        assertEquals(OperationKind.INT, structExpr.childValues[3].expressionKind);
        assertEquals(30L, structExpr.childValues[3].primitiveValue);
        
        assertEquals(OperationKind.FIELD_NAME, structExpr.childValues[4].expressionKind);
        assertEquals("active", structExpr.childValues[4].objectValue);
        
        assertEquals(OperationKind.BOOL, structExpr.childValues[5].expressionKind);
        assertEquals(1L, structExpr.childValues[5].primitiveValue); // true = 1
    }

    @Test
    public void testPlaceholders() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .listValue(list -> list
                .placeholder()
                .taglessPlaceholder(TaglessScalarType.INT)
                .taglessPlaceholder(TaglessScalarType.SYMBOL))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check list container with placeholders
        TemplateExpression listExpr = expressions.get(0);
        assertEquals(OperationKind.LIST, listExpr.expressionKind);
        assertEquals(3, listExpr.childValues.length);
        
        // Check tagged placeholder (placeholder)
        assertEquals(OperationKind.PLACEHOLDER, listExpr.childValues[0].expressionKind);
        assertEquals(0L, listExpr.childValues[0].primitiveValue); // tagged placeholder
        
        // Check tagless INT placeholder
        assertEquals(OperationKind.PLACEHOLDER, listExpr.childValues[1].expressionKind);
        assertEquals(TaglessScalarType.INT.getOpcode(), (int)listExpr.childValues[1].primitiveValue);
        
        // Check tagless SYMBOL placeholder
        assertEquals(OperationKind.PLACEHOLDER, listExpr.childValues[2].expressionKind);
        assertEquals(TaglessScalarType.SYMBOL.getOpcode(), (int)listExpr.childValues[2].primitiveValue);
    }

    @Test
    public void testPlaceholderWithDefault() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .listValue(list -> list
                .placeholderWithDefault(defaultBuilder -> defaultBuilder
                    .stringValue("default_value")))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check list container with placeholder with default
        TemplateExpression listExpr = expressions.get(0);
        assertEquals(OperationKind.LIST, listExpr.expressionKind);
        assertEquals(1, listExpr.childValues.length);
        
        // Check placeholder with default (placeholder with default value in childValues)
        TemplateExpression placeholderExpr = listExpr.childValues[0];
        assertEquals(OperationKind.PLACEHOLDER, placeholderExpr.expressionKind);
        assertEquals(0L, placeholderExpr.primitiveValue); // tagged placeholder
        assertEquals(1, placeholderExpr.childValues.length); // has default value
        
        // Check the default value
        TemplateExpression defaultExpr = placeholderExpr.childValues[0];
        assertEquals(OperationKind.STRING, defaultExpr.expressionKind);
        assertEquals("default_value", defaultExpr.objectValue);
    }

    @Test
    public void testAnnotations() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .annotated("annotation1", "annotation2")
            .stringValue("annotated_value")
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(2, expressions.size()); // ANNOTATIONS operation + annotated value
        
        // Check annotations operation
        TemplateExpression annotationsExpr = expressions.get(0);
        assertEquals(OperationKind.ANNOTATIONS, annotationsExpr.expressionKind);
        assertEquals(0, annotationsExpr.childValues.length); // Annotations are stored separately
        
        // Check annotated value
        TemplateExpression valueExpr = expressions.get(1);
        assertEquals(OperationKind.STRING, valueExpr.expressionKind);
        assertEquals("annotated_value", valueExpr.objectValue);
    }

    @Test
    public void testComplexExample() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .structValue(struct -> struct
                .fieldName("person")
                .structValue(person -> person
                    .fieldName("name")
                    .placeholder()
                    .fieldName("age")
                    .taglessPlaceholder(TaglessScalarType.INT)
                    .fieldName("addresses")
                    .listValue(addresses -> addresses
                        .structValue(address -> address
                            .fieldName("street")
                            .placeholder()
                            .fieldName("city")
                            .placeholder())))
                .fieldName("metadata")
                .annotated("timestamp")
                .placeholderWithDefault(defaultBuilder -> defaultBuilder.nullValue(IonType.TIMESTAMP)))
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check root struct
        TemplateExpression rootStruct = expressions.get(0);
        assertEquals(OperationKind.STRUCT, rootStruct.expressionKind);
        assertEquals(5, rootStruct.childValues.length); // Based on debug output
        
        // Check "person" field name and nested struct
        assertEquals(OperationKind.FIELD_NAME, rootStruct.childValues[0].expressionKind);
        assertEquals("person", rootStruct.childValues[0].objectValue);
        
        TemplateExpression personStruct = rootStruct.childValues[1];
        assertEquals(OperationKind.STRUCT, personStruct.expressionKind);
        assertEquals(6, personStruct.childValues.length); // 3 field names + 3 values
        
        // Check person struct contents
        assertEquals(OperationKind.FIELD_NAME, personStruct.childValues[0].expressionKind);
        assertEquals("name", personStruct.childValues[0].objectValue);
        
        assertEquals(OperationKind.PLACEHOLDER, personStruct.childValues[1].expressionKind);
        assertEquals(0L, personStruct.childValues[1].primitiveValue); // tagged placeholder
        
        assertEquals(OperationKind.FIELD_NAME, personStruct.childValues[2].expressionKind);
        assertEquals("age", personStruct.childValues[2].objectValue);
        
        assertEquals(OperationKind.PLACEHOLDER, personStruct.childValues[3].expressionKind);
        assertEquals(TaglessScalarType.INT.getOpcode(), (int)personStruct.childValues[3].primitiveValue);
        
        assertEquals(OperationKind.FIELD_NAME, personStruct.childValues[4].expressionKind);
        assertEquals("addresses", personStruct.childValues[4].objectValue);
        
        // Check addresses list
        TemplateExpression addressesList = personStruct.childValues[5];
        assertEquals(OperationKind.LIST, addressesList.expressionKind);
        assertEquals(1, addressesList.childValues.length);
        
        // Check address struct inside list
        TemplateExpression addressStruct = addressesList.childValues[0];
        assertEquals(OperationKind.STRUCT, addressStruct.expressionKind);
        assertEquals(4, addressStruct.childValues.length); // 2 field names + 2 values
        
        assertEquals(OperationKind.FIELD_NAME, addressStruct.childValues[0].expressionKind);
        assertEquals("street", addressStruct.childValues[0].objectValue);
        
        assertEquals(OperationKind.PLACEHOLDER, addressStruct.childValues[1].expressionKind);
        assertEquals(0L, addressStruct.childValues[1].primitiveValue); // tagged placeholder
        
        assertEquals(OperationKind.FIELD_NAME, addressStruct.childValues[2].expressionKind);
        assertEquals("city", addressStruct.childValues[2].objectValue);
        
        assertEquals(OperationKind.PLACEHOLDER, addressStruct.childValues[3].expressionKind);
        assertEquals(0L, addressStruct.childValues[3].primitiveValue); // tagged placeholder
        
        // Check "metadata" field name and annotated placeholder with default
        assertEquals(OperationKind.FIELD_NAME, rootStruct.childValues[2].expressionKind);
        assertEquals("metadata", rootStruct.childValues[2].objectValue);
        
        TemplateExpression annotatedExpr = rootStruct.childValues[3];
        assertEquals(OperationKind.ANNOTATIONS, annotatedExpr.expressionKind);
        assertEquals(0, annotatedExpr.childValues.length); // Annotations are stored separately, not as children
        
        // Check placeholder with default (the next expression after annotations)
        TemplateExpression varWithDefault = rootStruct.childValues[4];
        assertEquals(OperationKind.PLACEHOLDER, varWithDefault.expressionKind);
        assertEquals(0L, varWithDefault.primitiveValue); // tagged placeholder
        assertEquals(1, varWithDefault.childValues.length); // has default value
        
        // Check the default value (null timestamp)
        TemplateExpression defaultValue = varWithDefault.childValues[0];
        assertEquals(OperationKind.NULL, defaultValue.expressionKind);
        assertEquals(IonType.TIMESTAMP, defaultValue.objectValue);
    }

    @Test
    public void testBlobValue() {
        byte[] testData = "Hello, World!".getBytes();
        
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .blobValue(testData)
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check BLOB value
        TemplateExpression blobExpr = expressions.get(0);
        assertEquals(OperationKind.BLOB, blobExpr.expressionKind);
        assertTrue(blobExpr.objectValue instanceof ByteSlice);
        ByteSlice blobSlice = (ByteSlice) blobExpr.objectValue;
        assertEquals(testData.length, blobSlice.getLength());
    }

    @Test
    public void testClobValue() {
        byte[] testData = "Hello, World!".getBytes();
        
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .clobValue(testData)
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check CLOB value
        TemplateExpression clobExpr = expressions.get(0);
        assertEquals(OperationKind.CLOB, clobExpr.expressionKind);
        assertTrue(clobExpr.objectValue instanceof ByteSlice);
        ByteSlice clobSlice = (ByteSlice) clobExpr.objectValue;
        assertEquals(testData.length, clobSlice.getLength());
    }

    @Test
    public void testEmptyMacroThrowsException() {
        assertThrows(IonException.class, () -> {
            ((MacroBuilder.FinalState) MacroBuilder.newBuilder()).build();
        });
    }

    @Test
    public void testAllTaglessScalarTypes() {
        MacroImpl macro = (MacroImpl) MacroBuilder.newBuilder()
            .listValue(list -> {
                // Test all TaglessScalarType values to ensure they work
                for (TaglessScalarType type : TaglessScalarType.values()) {
                    list.taglessPlaceholder(type);
                }
            })
            .build();
        
        assertNotNull(macro);
        
        // Verify the macro's body expressions
        List<TemplateExpression> expressions = macro.getBodyExpressions();
        assertEquals(1, expressions.size());
        
        // Check list container with all tagless placeholder types
        TemplateExpression listExpr = expressions.get(0);
        assertEquals(OperationKind.LIST, listExpr.expressionKind);
        
        TaglessScalarType[] allTypes = TaglessScalarType.values();
        assertEquals(allTypes.length, listExpr.childValues.length);
        
        // Check each tagless placeholder type
        for (int i = 0; i < allTypes.length; i++) {
            TemplateExpression varExpr = listExpr.childValues[i];
            assertEquals(OperationKind.PLACEHOLDER, varExpr.expressionKind);
            assertEquals(allTypes[i].getOpcode(), (int)varExpr.primitiveValue);
        }
    }

}
