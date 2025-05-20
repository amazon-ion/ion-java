package com.amazon.ion.impl.macro;

public class ExpansionKinds {
    public static final byte UNINITIALIZED = 0;
    public static final byte EMPTY = 1;
    public static final byte STREAM = 2;
    public static final byte VARIABLE = 3;
    public static final byte TEMPLATE_BODY = 4;
    public static final byte EXPR_GROUP = 5;
    public static final byte EXACTLY_ONE_VALUE_STREAM = 6;
    public static final byte IF_NONE = 7;
    public static final byte IF_SOME = 8;
    public static final byte IF_SINGLE = 9;
    public static final byte IF_MULTI = 10;
    public static final byte ANNOTATE = 11;
    public static final byte MAKE_STRING = 12;
    public static final byte MAKE_SYMBOL = 13;
    public static final byte MAKE_BLOB = 14;
    public static final byte MAKE_DECIMAL = 15;
    public static final byte MAKE_TIMESTAMP = 16;
    public static final byte PRIVATE_MAKE_FIELD_NAME_AND_VALUE = 17;
    public static final byte PRIVATE_FLATTEN_STRUCT = 18;
    public static final byte FLATTEN = 19;
    public static final byte SUM = 20;
    public static final byte DELTA = 21;
    public static final byte REPEAT = 22;
}
