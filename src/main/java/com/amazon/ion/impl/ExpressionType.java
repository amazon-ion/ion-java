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
    DATA_MODEL_CONTAINER_END;

    public boolean isEnd() {
        return this == E_EXPRESSION_END || this == EXPRESSION_GROUP_END || this == DATA_MODEL_CONTAINER_END;
    }

    public boolean isContainerStart() {
        return this == E_EXPRESSION || this == EXPRESSION_GROUP || this == DATA_MODEL_CONTAINER;
    }
}
