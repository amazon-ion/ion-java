// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.TestUtils.GLOBAL_SKIP_LIST;
import static com.amazon.ion.TestUtils.NON_EQUIVS_IONTESTS_FILEs;

import com.amazon.ion.junit.Injected.Inject;
import java.io.File;

public class NonEquivsTest
    extends EquivsTestCase
{
    @Inject("testFile")
    public static final File[] FILES =
        TestUtils.testdataFiles(GLOBAL_SKIP_LIST, NON_EQUIVS_IONTESTS_FILEs);

    public NonEquivsTest()
    {
        super(false);
    }
}
