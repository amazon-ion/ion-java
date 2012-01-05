// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;

/**
 *
 */
public interface _Private_IonWriter
    extends IonWriter
{
    /** Mostly for testing at this point, but could be useful public API. */
    IonCatalog getCatalog();
}
