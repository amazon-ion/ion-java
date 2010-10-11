// Copyright (c) 2010 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.streaming.RoundTripStreamingTests.StreamingRoundTripTest;
import java.io.File;

/**
 *
 */
public class OneForDebuggingTest
    extends IonTestCase
{

    public void testOneRoundTripStreamingTest()
    {
        String code_dir = getProjectHome().getAbsolutePath();
        String root_name = "\\IonJava\\";
        int root = code_dir.indexOf(root_name);
        String test_dir = code_dir.substring(0, root);
        String remainder = code_dir.substring(root + root_name.length());
        test_dir += "\\IonTests\\"+ remainder + "\\iontestdata\\";
        String test_file_path = test_dir + "equivs\\symbols.ion";
        File f = new File(test_file_path);
        FileTestCase test = makeTest(f);
        test.run();
    }
    protected FileTestCase makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        // this test is here to get rid of the warning, and ... you never know
        if (fileName == null || fileName.length() < 1) throw new IllegalArgumentException("files should have names");
        return new StreamingRoundTripTest(ionFile);
    }


}
