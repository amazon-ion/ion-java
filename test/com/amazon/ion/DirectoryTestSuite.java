// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Base class for test suites constructed from the files in a single directory.
 *
 */
public abstract class DirectoryTestSuite
    extends TestSuite
{

    public DirectoryTestSuite(String... testdataDirs)
    {
        super();

        setName(getClass().getName());

        List<String> skip = Arrays.asList(getFilesToSkip());

        for (String testdataDir : testdataDirs)
        {
            File dir = IonTestCase.getTestdataFile(testdataDir);

            String[] fileNames = dir.list();
            if (fileNames == null)
            {
                String message =
                    "testdataDir is not a directory: "
                    + dir.getAbsolutePath();
                throw new IllegalArgumentException(message);
            }

            // Sort the fileNames so they are listed in order.
            Arrays.sort(fileNames);
            for (String fileName : fileNames)
            {

                File testFile = new File(dir, fileName);

                Test test = makeTest(testFile);
                if (test != null)
                {
                    String testName = ((TestCase)test).getName();
                    if (skip.contains(testName))
                    {
                        System.err.println("WARNING: " + getName()
                                           + " skipping " + testName);
                        continue;
                    }
                    addTest(test);
                }
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

    protected String[] getFilesToSkip()
    {
        return new String[0];
    }
}
