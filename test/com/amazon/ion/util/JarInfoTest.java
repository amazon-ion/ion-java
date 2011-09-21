// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 *
 */
public class JarInfoTest
{
    @Test
    public void testConstruction()
    {
        JarInfo info = new JarInfo();

        assertTrue(info.getReleaseLabel().startsWith("R1"));
        assertTrue(info.getBrazilMajorVersion().startsWith("1."));
        assertTrue(info.getBrazilPackageVersion().startsWith("IonJava-1."));
        assertNotNull(info.getBuildTime());
    }
}
