// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import org.junit.Test;

/**
 *
 */
public class JarInfoTest
{
    @Test
    public void testConstruction()
    {
        // This attempts to load the properties file, but within Eclipse its
        // not on the classpath.  At least we'll make sure the constructor
        // doesn't die when the file is missing.
        JarInfo info = new JarInfo();
    }
}
