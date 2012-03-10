// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

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

        // TODO ION-165 this shouldn't be needed.
        // If removed, the datagram can end up with extra IVM sometimes.

        // Datagrams have an implicit initial IVM
        _previous_value_was_ivm = true;
        // TODO what if container isn't a datagram?
    }
}
