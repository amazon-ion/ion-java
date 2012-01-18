// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.SymbolTable;

/**
 * NOT FOR APPLICATION USE!
 */
public interface _Private_IonDatagram
    extends _Private_IonValue, IonDatagram
{
    void appendTrailingSymbolTable(SymbolTable symtab);
}
