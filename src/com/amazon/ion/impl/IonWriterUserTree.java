// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonIterationType;



/**
 *
 */
public class IonWriterUserTree
    extends IonWriterUser
{
    /**
     * really this constructor is here to verify that the
     * user writer is constructed with the right type of
     * system writer - a tree writer.
     *
     * @param systemWriter a System Tree writer to back this
     */
    protected IonWriterUserTree(IonWriterSystemTree systemWriter)
    {
        super(systemWriter, systemWriter.get_root());
    }

    @Override
    public IonIterationType getIterationType()
    {
        return IonIterationType.USER_ION_VALUE;
    }
}
