/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

import com.amazon.ion.BadIonTests;
import com.amazon.ion.BinaryTest;
import com.amazon.ion.BlobTest;
import com.amazon.ion.BoolTest;
import com.amazon.ion.ClobTest;
import com.amazon.ion.DatagramTest;
import com.amazon.ion.DecimalTest;
import com.amazon.ion.EquivsTests;
import com.amazon.ion.FloatTest;
import com.amazon.ion.GoodIonTests;
import com.amazon.ion.IntTest;
import com.amazon.ion.ListTest;
import com.amazon.ion.LoaderTest;
import com.amazon.ion.NullTest;
import com.amazon.ion.RoundTripTests;
import com.amazon.ion.SexpTest;
import com.amazon.ion.StringTest;
import com.amazon.ion.StructTest;
import com.amazon.ion.SymbolTest;
import com.amazon.ion.TimestampTest;
import com.amazon.ion.impl.ByteBufferTest;
import com.amazon.ion.impl.CharacterReaderTest;
import com.amazon.ion.impl.IonEqualsTest;
import com.amazon.ion.impl.IterationTest;
import com.amazon.ion.impl.ReaderTest;
import com.amazon.ion.impl.SymbolTableTest;
import com.amazon.ion.streaming.BadIonStreamingTests;
import com.amazon.ion.streaming.BinaryStreamingTest;
import com.amazon.ion.streaming.GoodIonStreamingTests;
import com.amazon.ion.streaming.RoundTripStreamingTests;
import com.amazon.ion.system.SimpleCatalogTest;
import com.amazon.ion.util.EquivalenceTest;
import com.amazon.ion.util.PrinterTest;
import com.amazon.ion.util.TextTest;
import junit.framework.JUnit4TestAdapter;
import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * Runs all tests for the Ion project.
 */
public class AllTests
{
    public static Test suite()
    {
        TestSuite suite =
            new TestSuite("AllTests for Ion");

        //$JUnit-BEGIN$

        // Low-level facilities.
        suite.addTestSuite(ByteBufferTest.class);
        suite.addTestSuite(TextTest.class);
        suite.addTestSuite(CharacterReaderTest.class);

        // General framework tests
        suite.addTestSuite(SimpleCatalogTest.class);

        // Type-based DOM tests
        suite.addTestSuite(BlobTest.class);
        suite.addTestSuite(BoolTest.class);
        suite.addTestSuite(ClobTest.class);
        suite.addTestSuite(DecimalTest.class);
        suite.addTestSuite(FloatTest.class);
        suite.addTestSuite(IntTest.class);
        suite.addTestSuite(ListTest.class);
        suite.addTestSuite(NullTest.class);
        suite.addTestSuite(SexpTest.class);
        suite.addTestSuite(StringTest.class);
        suite.addTestSuite(StructTest.class);
        suite.addTestSuite(SymbolTest.class);
        suite.addTestSuite(TimestampTest.class);

        // binary format tests
        suite.addTestSuite(BinaryTest.class);

        // Utility tests
        suite.addTestSuite(LoaderTest.class);
        suite.addTestSuite(IterationTest.class);
        suite.addTestSuite(ReaderTest.class);
        suite.addTestSuite(PrinterTest.class);

        suite.addTestSuite(SymbolTableTest.class);
        suite.addTestSuite(DatagramTest.class);

        // equality testing
        suite.addTest(new JUnit4TestAdapter(EquivalenceTest.class));
        suite.addTest(new JUnit4TestAdapter(IonEqualsTest.class));

        // General processing test suite
        suite.addTest(new GoodIonTests());
        suite.addTest(new BadIonTests());
        suite.addTest(new EquivsTests());
        suite.addTest(new RoundTripTests());

        suite.addTestSuite(BinaryStreamingTest.class);
        suite.addTest(new BadIonStreamingTests());
        suite.addTest(new GoodIonStreamingTests());
        suite.addTest(new RoundTripStreamingTests());

        //$JUnit-END$

        return suite;
    }
}
