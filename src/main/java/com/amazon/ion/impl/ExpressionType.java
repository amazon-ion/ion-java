package com.amazon.ion.impl;

public enum ExpressionType {
    FIELD_NAME,
    ANNOTATION,
    E_EXPRESSION, // TODO might need a way to denote delimited vs prefixed logical containers, once we make it possible to not add the contents of prefixed containers to the tape
    E_EXPRESSION_END,
    EXPRESSION_GROUP,
    EXPRESSION_GROUP_END,
    DATA_MODEL_SCALAR,
    DATA_MODEL_CONTAINER,
    DATA_MODEL_CONTAINER_END,
    CONTINUE_EXPANSION,
    END_OF_EXPANSION,
    VARIABLE;

    public final byte ord;

    ExpressionType() {
        this.ord = (byte) ordinal();
    }

    public static final byte FIELD_NAME_ORDINAL = (byte) FIELD_NAME.ordinal();
    public static final byte ANNOTATION_ORDINAL = (byte) ANNOTATION.ordinal();
    public static final byte E_EXPRESSION_ORDINAL = (byte) E_EXPRESSION.ordinal();
    public static final byte E_EXPRESSION_END_ORDINAL = (byte) E_EXPRESSION_END.ordinal();
    public static final byte EXPRESSION_GROUP_ORDINAL = (byte) EXPRESSION_GROUP.ordinal();
    public static final byte EXPRESSION_GROUP_END_ORDINAL = (byte) EXPRESSION_GROUP_END.ordinal();
    public static final byte DATA_MODEL_SCALAR_ORDINAL = (byte) DATA_MODEL_SCALAR.ordinal();
    public static final byte DATA_MODEL_CONTAINER_ORDINAL = (byte) DATA_MODEL_CONTAINER.ordinal();
    public static final byte DATA_MODEL_CONTAINER_END_ORDINAL = (byte) DATA_MODEL_CONTAINER_END.ordinal();
    public static final byte CONTINUE_EXPANSION_ORDINAL = (byte) CONTINUE_EXPANSION.ordinal();
    public static final byte END_OF_EXPANSION_ORDINAL = (byte) END_OF_EXPANSION.ordinal();
    public static final byte VARIABLE_ORDINAL = (byte) VARIABLE.ordinal();


    public static boolean isEnd(byte ordinal) {
        return ordinal == E_EXPRESSION_END_ORDINAL || ordinal == EXPRESSION_GROUP_END_ORDINAL || ordinal == DATA_MODEL_CONTAINER_END_ORDINAL;
    }

    public static boolean isContainerStart(byte ordinal) {
        return ordinal == E_EXPRESSION_ORDINAL || ordinal == EXPRESSION_GROUP_ORDINAL || ordinal == DATA_MODEL_CONTAINER_ORDINAL;
    }

    public static boolean isDataModelExpression(byte ordinal) {
        return ordinal == DATA_MODEL_CONTAINER_ORDINAL || ordinal == DATA_MODEL_SCALAR_ORDINAL || ordinal == ANNOTATION_ORDINAL || ordinal == FIELD_NAME_ORDINAL;
    }

    public static boolean isDataModelValue(byte ordinal) {
        return ordinal == DATA_MODEL_CONTAINER_ORDINAL || ordinal == DATA_MODEL_SCALAR_ORDINAL;
    }

    // TODO optimize these utilities using ordinals

    public boolean isEnd() {
        return ord == E_EXPRESSION_END_ORDINAL || ord == EXPRESSION_GROUP_END_ORDINAL || ord == DATA_MODEL_CONTAINER_END_ORDINAL; // TODO END_OF_EXPANSION?
    }

    public boolean isContainerStart() {
        return ord == E_EXPRESSION_ORDINAL || ord == EXPRESSION_GROUP_ORDINAL || ord == DATA_MODEL_CONTAINER_ORDINAL;
    }

    public boolean isDataModelExpression() { // TODO difference between this and data model value?
        return ord == DATA_MODEL_CONTAINER_ORDINAL || ord == DATA_MODEL_SCALAR_ORDINAL || ord == ANNOTATION_ORDINAL || ord == FIELD_NAME_ORDINAL;
    }

    public boolean isDataModelValue() {
        return ord == DATA_MODEL_CONTAINER_ORDINAL || ord == DATA_MODEL_SCALAR_ORDINAL;
    }
}
