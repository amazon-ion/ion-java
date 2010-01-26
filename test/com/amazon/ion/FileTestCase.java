// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import java.io.File;


/**
 * Base class for test cases constructed from a file.
 * Implementations should override {@link #runTest} based on the value of
 * member {@link #myTestFile}.
 */
public abstract class FileTestCase
    extends IonTestCase
{
    protected File myTestFile;

    public FileTestCase(File testFile)
    {
        this(testFile.getParentFile().getName() + '/' + testFile.getName(),
             testFile);
    }

    public FileTestCase(String testName, File testFile)
    {
        super(testName);
        myTestFile = testFile;
    }


    /**
     * Subclasses should override this to implement the test case.
     * @see junit.framework.TestCase#runTest()
     */
    @Override
    protected abstract void runTest() throws Throwable;
}
