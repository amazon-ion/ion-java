/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import com.amazon.ion.IonException;
import com.amazon.ion.Timestamp;


/**
 * Provides information about this release of the ion-java library.
 */
public final class JarInfo
{
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
        loadBuildProperties();
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
    /**
     * @return null but not empty string
     */
    private static String nonEmptyProperty(Properties props, String name)
    {
        String value = props.getProperty(name, "");
        if (value.length() == 0) value = null;
        return value;
    }

    private void loadBuildProperties()
            throws IonException
    {
        String file = "/ion-java.properties";
        try
        {
            Properties props = new Properties();

            InputStream in = getClass().getResourceAsStream(file);
            if (in != null)
            {
                try
                {
                    props.load(in);
                }
                finally
                {
                    in.close();
                }
            }

            ourProjectVersion = nonEmptyProperty(props, "build.version");

            String time = nonEmptyProperty(props, "build.time");
            if (time != null)
            {
                try {
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
            throw new IonException("Unable to load " + file, e);
        }
    }
}