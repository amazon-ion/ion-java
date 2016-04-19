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
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import software.amazon.ion.IonException;
import software.amazon.ion.Timestamp;


/**
 * Provides information about this release of the ion-java library.
 */
public final class JarInfo
{

    private static final String MANIFEST_FILE = "/META-INF/MANIFEST.MF";

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
        loadBuildProperties(getClass().getResourceAsStream(MANIFEST_FILE));
    }

    JarInfo(InputStream in)
    {
        loadBuildProperties(in);
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

    private void loadBuildProperties(InputStream in) throws IonException
    {
        try
        {
            Manifest manifest = new Manifest();
            if (in != null)
            {
                try
                {
                    manifest.read(in);
                }
                finally
                {
                    in.close();
                }
            }
            Attributes mainAttributes = manifest.getMainAttributes();

            ourProjectVersion = mainAttributes.getValue("Project-Version");

            String time = mainAttributes.getValue("Build-Time");
            if (time != null)
            {
                try
                {
                    ourBuildTime = Timestamp.valueOf(time);
                }
                catch (IllegalArgumentException e)
                {
                    // Badly formatted timestamp. Ignore it.
                }
            }
        }
        catch (IOException e)
        {
            throw new IonException("Unable to load manifest.", e);
        }
    }
}
