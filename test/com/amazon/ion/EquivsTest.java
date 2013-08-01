// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.EQUIVS_IONTESTS_FILES;
import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;

public class EquivsTest
    extends EquivsTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        TestUtils.testdataFiles(GLOBAL_SKIP_LIST, EQUIVS_IONTESTS_FILES);

    public EquivsTest()
    {
        super(true);
    }
}
