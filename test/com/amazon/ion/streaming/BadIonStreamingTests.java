package com.amazon.ion.streaming;

import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestSuite;

public class BadIonStreamingTests extends DirectoryTestSuite {
    private static class BadIonStreamingTestCase
    	extends FileTestCase
    {
    	private final static boolean _debug_output_errors = false;
    	private final boolean myFileIsBinary;

    	public BadIonStreamingTestCase(File ionFile, boolean binary)
    	{
    	    super(ionFile);
    	    myFileIsBinary = binary;
    	}

    	@Override
        public void runTest()
    	    throws Exception
    	{
    	    try {
    	        iterateIon();
    	        fail("Expected IonException parsing "
    	             + myTestFile.getAbsolutePath());
    	    } catch (IonException e) {
    	        /* good - we're expecting an error, there are testing bad input */
    	        if (_debug_output_errors) {
    	            System.out.print(this.myTestFile.getName());
    	            System.out.print(": ");
    	            System.out.println(e.getMessage());
    	        }
    	    }
    	}

        void iterateIon() {
            IonReader it;
            int len = (int)myTestFile.length();
            byte[] buf = new byte[len];

            FileInputStream in;
            BufferedInputStream bin;
            try {
                in = new FileInputStream(myTestFile);
                bin = new BufferedInputStream(in);
                bin.read(buf);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            it = system().newReader(buf);
            // Do we want to check the type of "it" here
            // to make sure the iterator made the right
            // choice of binary or text?  Or should be test
            // that somewhere else?
            readEverything(it);
        }

        void readEverything(IonReader it) {
            while (it.hasNext()) {
                IonType t = it.next();
                switch (t) {
                    case NULL:
                    case BOOL:
                    case INT:
                    case FLOAT:
                    case DECIMAL:
                    case TIMESTAMP:
                    case STRING:
                    case SYMBOL:
                    case BLOB:
                    case CLOB:
                        if (this.myFileIsBinary) break; // really a no op to shut up the warning
                        break;
                    case STRUCT:
                    case LIST:
                    case SEXP:
                        it.stepInto();
                        readEverything(it);
                        it.stepOut();
                        break;
                    case DATAGRAM:
                        fail("datagram not expected");
                }
            }
            return;
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
}
