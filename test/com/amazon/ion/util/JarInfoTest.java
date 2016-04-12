// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JarInfoTest
{
    @Test
    public void testConstruction()
    {
        JarInfo info = new JarInfo();

        // When running on a laptop that can't run our Ant targets (due to
        // unavailable Brazil CLI and HappyTrails stuff) this test will fail.
        // Adding -DNOBRAZIL to the Eclipse Installed JRE will allow us to
        // succeed in that case.
        if (System.getProperty("NOBRAZIL") != null) return;

        assertTrue(info.getReleaseLabel().startsWith("R2"));
        assertTrue(info.getBrazilMajorVersion().startsWith("1."));
        assertTrue(info.getBrazilPackageVersion().startsWith("IonJava-1."));
        assertNotNull(info.getBuildTime());
    }
}
