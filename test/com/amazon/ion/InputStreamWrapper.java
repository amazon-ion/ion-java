// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.IOException;
import java.io.InputStream;

/**
 * Callback for decorating an {@link InputStream}.
 */
public interface InputStreamWrapper
{
    InputStream wrap(InputStream in) throws IOException;
}
