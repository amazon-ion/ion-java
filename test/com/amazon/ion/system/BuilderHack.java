// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;

/**
 * Workaround access control since
 * {@link IonSystemBuilder#setBinaryBacked(boolean)} isn't public yet.
 */
public class BuilderHack
{
    public static void setBinaryBacked(IonSystemBuilder b, boolean bb)
    {
        b.setBinaryBacked(bb);
    }
}
