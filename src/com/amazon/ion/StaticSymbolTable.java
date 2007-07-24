/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

/**
 *
 */
public interface StaticSymbolTable
    extends SymbolTable
{

    /**
     * Gets the unique name of this symbol table.
     *
     * @return the unique name.
     */
    public String getName();

    /**
     * Gets the version of this symbol table.
     *
     * @return at least one.
     */
    public int getVersion();

}
