// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.impl;
/** * Provides a simple implementation of {@link IonReaderPosition} * that delegates facet interpolation as a cast. */public abstract class IonReaderPositionBase implements IonReaderPosition{    public final <T> T asFacet(final Class<T> type)    {        if (!type.isInstance(this))        {            return null;        }        return type.cast(this);    }}
