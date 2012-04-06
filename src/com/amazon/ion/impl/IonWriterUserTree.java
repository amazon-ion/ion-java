// Copyright (c) 2010-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.ValueFactory;


/**
 *
 */
final class IonWriterUserTree
    extends IonWriterUser
{
    /**
     * really this constructor is here to verify that the
     * user writer is constructed with the right type of
     * system writer - a tree writer.
     * @param catalog may be null.
     * @param systemWriter a System Tree writer to back this.
     *   Must not be null.
     */
    protected IonWriterUserTree(IonCatalog catalog,
                                ValueFactory symtabValueFactory,
                                IonWriterSystemTree systemWriter)
    {
        super(catalog, symtabValueFactory, systemWriter);
    }
}
