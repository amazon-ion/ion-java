// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;

public class NonEquivsTest
    extends EquivsTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        TestUtils.testdataFiles(TestUtils.GLOBAL_SKIP_LIST, "good/non-equivs");

    public NonEquivsTest()
    {
        super(false);
    }
}
