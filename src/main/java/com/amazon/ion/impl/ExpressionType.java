package com.amazon.ion.impl;

public enum ExpressionType {
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
    VARIABLE,
    TOMBSTONE,
    NONE;

    public static final byte ANNOTATION_ORDINAL = 0; //(byte) ANNOTATION.ordinal();
    public static final byte E_EXPRESSION_ORDINAL = 1; // (byte) E_EXPRESSION.ordinal();
    public static final byte E_EXPRESSION_END_ORDINAL = 2; //(byte) E_EXPRESSION_END.ordinal();
    public static final byte EXPRESSION_GROUP_ORDINAL = 3; //(byte) EXPRESSION_GROUP.ordinal();
    public static final byte EXPRESSION_GROUP_END_ORDINAL = 4; //(byte) EXPRESSION_GROUP_END.ordinal();
    public static final byte DATA_MODEL_SCALAR_ORDINAL = 5; //(byte) DATA_MODEL_SCALAR.ordinal();
    public static final byte DATA_MODEL_CONTAINER_ORDINAL = 6; //(byte) DATA_MODEL_CONTAINER.ordinal();
    public static final byte DATA_MODEL_CONTAINER_END_ORDINAL = 7; //(byte) DATA_MODEL_CONTAINER_END.ordinal();
    public static final byte CONTINUE_EXPANSION_ORDINAL = 8; //(byte) CONTINUE_EXPANSION.ordinal();
    public static final byte END_OF_EXPANSION_ORDINAL = 9; //(byte) END_OF_EXPANSION.ordinal();
    public static final byte VARIABLE_ORDINAL = 10; //(byte) VARIABLE.ordinal();
    public static final byte TOMBSTONE_ORDINAL = 11;
    public static final byte NONE_ORDINAL = 12;


    private static final ExpressionType[] VALUES = ExpressionType.values();

    public static ExpressionType forOrdinal(int ordinal) {
        return VALUES[ordinal];
    }

    public static boolean isEnd(byte ordinal) {
        return ordinal == E_EXPRESSION_END_ORDINAL || ordinal == EXPRESSION_GROUP_END_ORDINAL || ordinal == DATA_MODEL_CONTAINER_END_ORDINAL;
    }

    public static boolean isContainerStart(byte ordinal) {
        return ordinal == E_EXPRESSION_ORDINAL || ordinal == EXPRESSION_GROUP_ORDINAL || ordinal == DATA_MODEL_CONTAINER_ORDINAL;
    }

    public static boolean isDataModelExpression(byte ordinal) {
        return ordinal == DATA_MODEL_CONTAINER_ORDINAL || ordinal == DATA_MODEL_SCALAR_ORDINAL || ordinal == ANNOTATION_ORDINAL;
    }

    public static boolean isDataModelValue(byte ordinal) {
        return ordinal == DATA_MODEL_CONTAINER_ORDINAL || ordinal == DATA_MODEL_SCALAR_ORDINAL;
    }
}
