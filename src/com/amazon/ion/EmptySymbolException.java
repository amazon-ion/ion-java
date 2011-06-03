/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

/**
 *
 */
public class EmptySymbolException
    extends IonException
{
    private static final long serialVersionUID = -7801632953459636349L;

    public EmptySymbolException()
    {
        super("Symbols must contain at least one character.");
    }
}
