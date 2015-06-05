// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import java.io.IOException;

/**
 * An {@link IonReader} {@linkplain com.amazon.ion.facet facet} that can rapidly bulk-copy
 * Ion binary data under certain circumstances.
 */
interface ByteTransferReader
{
    public void transferCurrentValue(_Private_ByteTransferSink writer)
        throws IOException;
}
