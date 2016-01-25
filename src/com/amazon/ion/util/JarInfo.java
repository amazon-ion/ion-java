// Copyright (c) 2011-2016 Amazon.com, Inc. All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.IonException;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Provides information about this release of the IonJava library.
 *
 * @since IonJava R13
 */
public final class JarInfo {

  private static final String MANIFEST_FILE = "/META-INF/MANIFEST.MF";

  private String ourProjectVersion;
  private Timestamp ourBuildTime;

  /**
   * Constructs a new instance that can provide build information about this library.
   *
   * @throws IonException if there's a problem loading the build info.
   */
  public JarInfo() throws IonException {
    loadBuildProperties(getClass().getResourceAsStream(MANIFEST_FILE));
  }

  JarInfo(InputStream in) {
    loadBuildProperties(in);
  }

  /**
   * Gets the ion-java project version of this build.
   *
   * @return null if the package version is unknown.
   */
  public String getProjectVersion() {
    return ourProjectVersion;
  }

  /**
   * Gets the time at which this package was built.
   *
   * @return null if the build time is unknown.
   */
  public Timestamp getBuildTime() {
    return ourBuildTime;
  }

  // TODO writeTo(IonWriter)

  // ========================================================================

  private void loadBuildProperties(InputStream in) throws IonException {
    try {
      Manifest manifest = new Manifest();
      if (in != null) {
        try {
          manifest.read(in);
        } finally {
          in.close();
        }
      }
      Attributes mainAttributes = manifest.getMainAttributes();

      ourProjectVersion = mainAttributes.getValue("Project-Version");

      String time = mainAttributes.getValue("Build-Time");
      if (time != null) {
        try {
          ourBuildTime = Timestamp.valueOf(time);
        } catch (IllegalArgumentException e) {
          // Badly formatted timestamp. Ignore it.
        }
      }
    } catch (IOException e) {
      throw new IonException("Unable to load manifest.", e);
    }
  }
}
