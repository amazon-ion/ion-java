// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.Faceted;
import com.amazon.ion.IonReader;
import java.io.IOException;

/**
 * An {@link IonReader} {@linkplain Faceted facet} that can rapidly bulk-copy
 * Ion binary data under certain circumstances.
 */
interface ByteTransferReader
{
    public void transferCurrentValue(IonWriterSystemBinary writer)
        throws IOException;
}
