// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonValue;

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Internal, private, interfaces for manipulating
 * the base child collection of IonContainer
 */
public interface _Private_IonContainer
    extends IonContainer
{
    public int      get_child_count();
    public IonValue get_child(int idx);
}
