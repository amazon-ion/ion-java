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

package software.amazon.ion.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import org.junit.Test;
import software.amazon.ion.Timestamp;
import software.amazon.ion.util.JarInfo;

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
