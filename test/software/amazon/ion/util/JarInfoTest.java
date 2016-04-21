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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.jar.Manifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import software.amazon.ion.IonException;
import software.amazon.ion.Timestamp;

public class JarInfoTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static String getAttributes(String manifestVersion)
    {
        return "Manifest-Version: " + manifestVersion + "\n";
    }

    private static String getIonJavaAttributes(String buildTime, String version)
    {
        return getAttributes("1.0")
            + "Ion-Java-Build-Time: " + buildTime + "\n"
            + "Ion-Java-Project-Version: " + version + "\n";
    }

    private static Manifest getManifest(String attributes) throws IOException
    {
        return new Manifest(new ByteArrayInputStream(attributes.getBytes("UTF-8")));
    }

    @Test
    public void testSingleManifest() throws Exception
    {
        String expectedBuildTime = "1984T";
        String expectedVersion = "42.0";

        Manifest manifest = getManifest(getIonJavaAttributes(expectedBuildTime, expectedVersion));
        JarInfo info = new JarInfo(Collections.singletonList(manifest));

        assertEquals(expectedVersion, info.getProjectVersion());
        assertEquals(Timestamp.valueOf(expectedBuildTime), info.getBuildTime());
    }

    @Test
    public void testMultipleManifests() throws Exception
    {
        String expectedBuildTime = "1984T";
        String expectedVersion = "42.0";

        List<Manifest> manifests = new ArrayList<Manifest>();
        manifests.add(getManifest(getAttributes("1.0")));
        manifests.add(getManifest(getIonJavaAttributes(expectedBuildTime, expectedVersion)));
        manifests.add(getManifest(getAttributes("2.0")));

        JarInfo info = new JarInfo(manifests);

        assertEquals(expectedVersion, info.getProjectVersion());
        assertEquals(Timestamp.valueOf(expectedBuildTime), info.getBuildTime());
    }

    @Test
    public void testNoMatchingManifests() throws Exception
    {
        List<Manifest> manifests = new ArrayList<Manifest>();
        manifests.add(getManifest(getAttributes("1.0")));
        manifests.add(getManifest(getAttributes("2.0")));
        manifests.add(getManifest(getAttributes("3.0")));

        thrown.expect(IonException.class);
        new JarInfo(manifests);
    }

    @Test
    public void testConflictingManifests() throws Exception
    {
        List<Manifest> manifests = new ArrayList<Manifest>();
        manifests.add(getManifest(getIonJavaAttributes("1984T", "42.0")));
        manifests.add(getManifest(getAttributes("1.0")));
        manifests.add(getManifest(getIonJavaAttributes("1985T", "43.0")));

        thrown.expect(IonException.class);
        new JarInfo(manifests);
    }

}
