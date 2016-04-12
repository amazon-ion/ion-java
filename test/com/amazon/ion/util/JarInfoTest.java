/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

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
