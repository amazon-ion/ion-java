// Copyright (c) 2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;

/**
 *  IonException cover for errors encountered using the
 *  binary reader (july 2009 version).
 */
public class IonReaderBinaryExceptionX
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public IonReaderBinaryExceptionX(Exception e) {
        super(e);
    }
    public IonReaderBinaryExceptionX(String msg) {
        super(msg);
    }
}
