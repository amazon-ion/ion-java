// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import java.io.IOException;

/**
 * An {@link IonReader} {@linkplain com.amazon.ion.facet facet} that can rapidly bulk-copy
 * Ion binary data under certain circumstances.
 *
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public interface PrivateByteTransferReader
{
    public void transferCurrentValue(PrivateByteTransferSink writer)
        throws IOException;
}
