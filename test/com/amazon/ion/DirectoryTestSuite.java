/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.File;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Base class for test suites constructed from the files in a single directory.
 *
 */
public abstract class DirectoryTestSuite
    extends TestSuite
{

    public DirectoryTestSuite(String testdataDir)
    {
        super();

        File goodFilesDir = IonTestCase.getTestdataFile(testdataDir);
        String[] list = goodFilesDir.list();
        String[] fileNames = list;

        // Sort the fileNames so they are listed in order.
        Arrays.sort(fileNames);
        for (String fileName : fileNames)
        {
            File testFile = new File(goodFilesDir, fileName);

            Test test = makeTest(testFile);
            if (test != null)
            {
                addTest(test);
            }
        }
    }

    /**
     * Creates a test case from one of the files in this suite's directory.
     *
     * @param testFile is a file in the directory of this suite.
     * It must not be <code>null</code>.
     * @return the test case, or <code>null</code> if the file should be
     * ignored.
     */
    protected abstract Test makeTest(File testFile);
}
