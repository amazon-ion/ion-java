// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

/**
 * An error caused by a request for an Ion version that is not supported by
 * this implementation.
 */
public class UnsupportedIonVersionException
    extends IonException
{
    private static final long serialVersionUID = -1166749371823975664L;

    private final String _unsupportedIonVersionId;

    public UnsupportedIonVersionException(String unsupportedIonVersionId)
    {
        _unsupportedIonVersionId = unsupportedIonVersionId;
    }

    public String getUnsuportedIonVersionId()
    {
        return _unsupportedIonVersionId;
    }

    @Override
    public String getMessage()
    {
        return "Unsupported Ion version " + _unsupportedIonVersionId;
    }
}
