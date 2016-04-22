/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import software.amazon.ion.IonException;
import software.amazon.ion.Timestamp;


/**
 * Provides information about this release of the ion-java library.
 */
public final class JarInfo
{

    private static final String MANIFEST_FILE = "META-INF/MANIFEST.MF";
    private static final String BUILD_TIME_ATTRIBUTE = "Ion-Java-Build-Time";
    private static final String PROJECT_VERSION_ATTRIBUTE = "Ion-Java-Project-Version";

    private String ourProjectVersion;

    private Timestamp ourBuildTime;

    /**
     * Constructs a new instance that can provide build information about this
     * library.
     *
     * @throws IonException
     *         if there's a problem loading the build info.
     */
    public JarInfo() throws IonException
    {
        Enumeration<URL> manifestUrls;
        try
        {
            manifestUrls = getClass().getClassLoader().getResources(MANIFEST_FILE);
        }
        catch (IOException e)
        {
            throw new IonException("Unable to load manifests.", e);
        }
        List<Manifest> manifests = new ArrayList<Manifest>();
        while (manifestUrls.hasMoreElements())
        {
            try
            {
                manifests.add(new Manifest(manifestUrls.nextElement().openStream()));
            }
            catch (IOException e)
            {
                continue; // try the next manifest
            }
        }
        loadBuildProperties(manifests);
    }

    JarInfo(List<Manifest> manifests)
    {
        loadBuildProperties(manifests);
    }

    /**
     * Gets the ion-java project version of this build.
     *
     * @return null if the package version is unknown.
     */
    public String getProjectVersion()
    {
        return ourProjectVersion;
    }

    /**
     * Gets the time at which this package was built.
     *
     * @return null if the build time is unknown.
     */
    public Timestamp getBuildTime()
    {
        return ourBuildTime;
    }

    // TODO writeTo(IonWriter)

    // ========================================================================

    private void loadBuildProperties(List<Manifest> manifests) throws IonException
    {
        boolean propertiesLoaded = false;
        for(Manifest manifest : manifests)
        {
            boolean success = tryLoadBuildProperties(manifest);
            if(success && propertiesLoaded)
            {
                // In the event of conflicting manifests, fail instead of risking returning incorrect version info.
                throw new IonException("Found multiple manifests with ion-java version info on the classpath.");
            }
            propertiesLoaded |= success;
        }
        if (!propertiesLoaded)
        {
            throw new IonException("Unable to locate manifest with ion-java version info on the classpath.");
        }
    }

    /*
     * Returns true if the properties were loaded, otherwise false.
     */
    private boolean tryLoadBuildProperties(Manifest manifest)
    {
        Attributes mainAttributes = manifest.getMainAttributes();
        String projectVersion = mainAttributes.getValue(PROJECT_VERSION_ATTRIBUTE);
        String time = mainAttributes.getValue(BUILD_TIME_ATTRIBUTE);

        if (projectVersion == null || time == null)
        {
            return false;
        }

        ourProjectVersion = projectVersion;

        try
        {
            ourBuildTime = Timestamp.valueOf(time);
        }
        catch (IllegalArgumentException e)
        {
            // Badly formatted timestamp. Ignore it.
        }
        return true;
    }
}
