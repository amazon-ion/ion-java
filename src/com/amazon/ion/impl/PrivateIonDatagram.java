// Copyright (c) 2011-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonDatagram;
import com.amazon.ion.SymbolTable;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateIonDatagram
    extends PrivateIonValue, IonDatagram
{
    void appendTrailingSymbolTable(SymbolTable symtab);
}
