// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolTable;

/**
 *
 */
public interface IonContainerPrivate
    extends IonContainer
{
    /**
     * Internal, private, interfaces for manipulating
     * the base child collection of IonContainer
     */
    public int      get_child_count();
    public IonValue get_child(int idx);
    public IonValue set_child(int idx, IonValue child);
    public int      add_child(int idx, IonValue child);
    public void     remove_child(int idx);
    public int      find_Child(IonValue child);

    public void     setSymbolTable(SymbolTable symbols);
}
