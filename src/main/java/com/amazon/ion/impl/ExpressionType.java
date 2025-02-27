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

    // TODO optimize these utilities using ordinals

    public boolean isEnd() {
        return this == E_EXPRESSION_END || this == EXPRESSION_GROUP_END || this == DATA_MODEL_CONTAINER_END; // TODO END_OF_EXPANSION?
    }

    public boolean isContainerStart() {
        return this == E_EXPRESSION || this == EXPRESSION_GROUP || this == DATA_MODEL_CONTAINER;
    }

    public boolean isDataModelExpression() { // TODO difference between this and data model value?
        return this == DATA_MODEL_CONTAINER || this == DATA_MODEL_SCALAR || this == ANNOTATION || this == FIELD_NAME;
    }

    public boolean isDataModelValue() {
        return this == DATA_MODEL_CONTAINER || this == DATA_MODEL_SCALAR;
    }
}
