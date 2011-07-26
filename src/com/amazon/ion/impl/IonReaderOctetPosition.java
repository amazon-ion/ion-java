// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;


/**
 * Provides octet level positioning information for an {@link IonReaderPosition}.
 */
public interface IonReaderOctetPosition
    extends IonReaderPosition
{
    /** Returns the octet position in the logical byte stream sourcing the position. */
    public long getOffset();

    /** Returns the octet length of the value sourcing the position. */
    public long getLength();
}
