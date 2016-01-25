// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static org.junit.Assert.assertEquals;

import com.amazon.ion.Timestamp;
import java.io.ByteArrayInputStream;
import org.junit.Test;

public class JarInfoTest
{
    @Test
    public void testConstruction() throws Exception
    {
        String expectedBuildTime = "1984T";
        String expectedVersion = "42.0";

        String manifest = "Manifest-Version: 1.0\n"
                        + "Build-Time: " + expectedBuildTime + "\n"
                        + "Project-Version: " + expectedVersion + "\n";

        JarInfo info = new JarInfo(new ByteArrayInputStream(manifest.getBytes("UTF-8")));

        assertEquals(expectedVersion, info.getProjectVersion());
        assertEquals(Timestamp.valueOf(expectedBuildTime), info.getBuildTime());
    }
}
