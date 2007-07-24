/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

/**
 * Signals a request for an Ion system version that is not supported by this
 * implementation.
 */
public class UnsupportedSystemVersionException
    extends IonException
{
    private static final long serialVersionUID = -1166749371823975664L;

    private final String _unsupportedSystemId;

    public UnsupportedSystemVersionException(String unsupportedSystemId)
    {
        _unsupportedSystemId = unsupportedSystemId;
    }

    public String getUnsuportedSystemId()
    {
        return _unsupportedSystemId;
    }

    @Override
    public String getMessage()
    {
        return "Unsupported system version " + _unsupportedSystemId;
    }
}
