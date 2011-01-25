// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion.streaming;

import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.TestUtils;
import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.system.SystemFactory.SystemCapabilities;
import java.io.File;
import java.io.IOException;
import junit.framework.TestSuite;

public class BadIonStreamingTests extends DirectoryTestSuite {
    private static class BadIonStreamingTestCase
        extends FileTestCase
    {
        private final static boolean _debug_output_errors = false;

        public BadIonStreamingTestCase(File ionFile, boolean binary)
        {
            super(ionFile);
        }

        @Override
        public void runTest()
            throws Exception
        {
            iterateIon( true );

            if (getSystemCapabilities() == SystemCapabilities.LITE)
            {
                // "Lite" system doesn't validate while skipping scalars
                // so we won't throw exceptions for all bad files.
                iterateIon( false );
            }
        }

        void iterateIon(boolean materializeScalars)
        throws IOException
        {
            try {
                byte[] buf = IonImplUtils.loadFileBytes(myTestFile);

                // Do we want to check the type of "it" here
                // to make sure the iterator made the right
                // choice of binary or text?  Or should be test
                // that somewhere else?
                IonReader it = system().newReader(buf);
                TestUtils.deepRead(it, materializeScalars);
                fail("Expected IonException parsing "
                    + myTestFile.getAbsolutePath() + " (" + ( materializeScalars ? "" : "not " ) + "materializing scalars)");
           } catch (IonException e) {
               /* good - we're expecting an error, there are testing bad input */
               if (_debug_output_errors) {
                   System.out.print(this.myTestFile.getName());
                   System.out.print(": ");
                   System.out.println(e.getMessage());
               }
           }
        }
    }

    public static TestSuite suite() {
        return new BadIonStreamingTests();
    }

    public BadIonStreamingTests() {
        super("bad");
    }


    @Override
    protected BadIonStreamingTestCase makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        if (fileName.endsWith(".ion"))
        {
            return new BadIonStreamingTestCase(ionFile, false);
        }
        else if (fileName.endsWith(".10n"))
        {
            return new BadIonStreamingTestCase(ionFile, true);
        }
        return null;
    }

    @Override
    protected String[] getFilesToSkip()
    {
        return new String[]
        {
            // "annotationNan.ion",
            // "clob_U0000003F.ion",
            // "clob_U00000080.ion",
            // "clob_u0020.ion",
            // "clob_u00FF.ion",
            // "fieldNameNan.ion",
            // "hexWithTerminatingUtf8.ion",
            // "symbolEmptyWithCRLF.ion",
            // "symbolEmptyWithLF.ion",
            // "symbolEmptyWithLFLF.ion",
        };
    }
}
