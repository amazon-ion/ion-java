/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.system;

import com.amazon.ion.IonSystem;


/**
 * The standard, public implementation of Ion.
 *
 * @deprecated This class will be removed very soon.  Use {@link SystemFactory}
 * to construct an {@link IonSystem}.
 */
@Deprecated
public class StandardIonSystem
    extends com.amazon.ion.impl.IonSystemImpl
{
    public StandardIonSystem()
    {
    }
}
