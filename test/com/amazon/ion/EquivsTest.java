// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;

public class EquivsTest
    extends EquivsTestCase
{
    /**
     * TODO ION-314 IonTests "equivs" folder is to be moved to "good/equivs".
     */
    @Inject("testFile")
    public static final File[] FILES =
        TestUtils.testdataFiles(TestUtils.GLOBAL_SKIP_LIST, "equivs");

    public EquivsTest()
    {
        super(true);
    }
}
