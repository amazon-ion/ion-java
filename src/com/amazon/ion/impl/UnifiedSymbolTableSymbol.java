// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

/**
 *
 */
public class UnifiedSymbolTableSymbol
{
        public  final int                sid;
        public  final String             name;
        private       int                name_len;
        private       int                td_len;
        public  final UnifiedSymbolTable source;

        public UnifiedSymbolTableSymbol(String symbolName, int symbolId,
                      UnifiedSymbolTable sourceTable)
        {
            name   = symbolName;
            sid    = symbolId;
            source = sourceTable;
            name_len = -1;
            td_len   = -1;
        }
        private void init_lengths() {
            name_len = IonBinary.lenIonString(name);
            td_len   = (IonBinary.lenLenFieldWithOptionalNibble(name_len)
                        + IonConstants.BB_TOKEN_LEN);
        }
        public final int getNameLen() {
            if (name_len < 0) {
                init_lengths();
            }
            return name_len;
        }
        public final int getTdLen() {
            if (td_len < 0) {
                init_lengths();
            }
            return td_len;
        }

        @Override
        public String toString() {
            return "Symbol:"+sid+(name != null ? "-"+name : "");
        }
}
