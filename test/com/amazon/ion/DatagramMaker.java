// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * Abstracts the various ways that {@link IonDatagram}s can be created, so test
 * cases can cover all the APIs.
 */
public enum DatagramMaker
{
    /**
     * Invokes {@link IonLoader#load(String)}.
     */
    FROM_STRING()
    {
        @Override
        public IonDatagram newDatagram(IonSystem system, String ionText)
        {
            IonLoader loader = system.getLoader();
            IonDatagram datagram = loader.load(ionText);
            return datagram;
        }
    };

    public abstract IonDatagram newDatagram(IonSystem system, String ionText);
}
