// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

import com.amazon.ion.AnnotationEscapesTest;
import com.amazon.ion.AssertionsEnabledTest;
import com.amazon.ion.BadIonTest;
import com.amazon.ion.BinaryTest;
import com.amazon.ion.BlobTest;
import com.amazon.ion.BoolTest;
import com.amazon.ion.ClobTest;
import com.amazon.ion.DatagramTest;
import com.amazon.ion.DecimalTest;
import com.amazon.ion.EquivsTest;
import com.amazon.ion.ExtendedDecimalTest;
import com.amazon.ion.FieldNameEscapesTest;
import com.amazon.ion.FloatTest;
import com.amazon.ion.GoodIonTest;
import com.amazon.ion.HashCodeCorrectnessTest;
import com.amazon.ion.HashCodeDistributionTest;
import com.amazon.ion.IntTest;
import com.amazon.ion.IonExceptionTest;
import com.amazon.ion.IonSystemTest;
import com.amazon.ion.JavaNumericsTest;
import com.amazon.ion.ListTest;
import com.amazon.ion.LoaderTest;
import com.amazon.ion.LongStringTest;
import com.amazon.ion.NullTest;
import com.amazon.ion.RoundTripTest;
import com.amazon.ion.SexpTest;
import com.amazon.ion.StringFieldNameEscapesTest;
import com.amazon.ion.StringTest;
import com.amazon.ion.StructTest;
import com.amazon.ion.SurrogateEscapeTest;
import com.amazon.ion.SymbolTest;
import com.amazon.ion.SystemProcessingTests;
import com.amazon.ion.TimestampTest;
import com.amazon.ion.ValueFactorySequenceTest;
import com.amazon.ion.impl.ByteBufferTest;
import com.amazon.ion.impl.CharacterReaderTest;
import com.amazon.ion.impl.IonEqualsTest;
import com.amazon.ion.impl.IonWriterTests;
import com.amazon.ion.impl.IterationTest;
import com.amazon.ion.impl.ReaderTest;
import com.amazon.ion.impl.SymbolTableTest;
import com.amazon.ion.impl.TreeReaderTest;
import com.amazon.ion.streaming.BadIonStreamingTest;
import com.amazon.ion.streaming.BinaryStreamingTest;
import com.amazon.ion.streaming.GoodIonStreamingTest;
import com.amazon.ion.streaming.MiscStreamingTest;
import com.amazon.ion.streaming.ReaderPositioningTest;
import com.amazon.ion.streaming.ReaderSkippingTest;
import com.amazon.ion.streaming.RoundTripStreamingTest;
import com.amazon.ion.system.IonSystemBuilderTest;
import com.amazon.ion.system.SimpleCatalogTest;
import com.amazon.ion.util.EquivalenceTest;
import com.amazon.ion.util.IonStreamUtilsTest;
import com.amazon.ion.util.PrinterTest;
import com.amazon.ion.util.TextTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Runs all tests for the Ion project.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Low-level facilities.
    AssertionsEnabledTest.class,
    IonExceptionTest.class,
    ByteBufferTest.class,
    TextTest.class,
    CharacterReaderTest.class,
    JavaNumericsTest.class,
    ExtendedDecimalTest.class,

    // General framework tests
    SimpleCatalogTest.class,

    // Type-based DOM tests
    BlobTest.class,
    BoolTest.class,
    ClobTest.class,
    DatagramTest.class,
    DecimalTest.class,
    FloatTest.class,
    IntTest.class,
    ListTest.class,
    NullTest.class,
    SexpTest.class,
    StringTest.class,
    LongStringTest.class,
    StructTest.class,
    SymbolTest.class,
    TimestampTest.class,

    AnnotationEscapesTest.class,
    FieldNameEscapesTest.class,
    StringFieldNameEscapesTest.class,
    SurrogateEscapeTest.class,

    // binary format tests
    BinaryTest.class,

    // Utility tests
    LoaderTest.class,
    IterationTest.class,
    ReaderTest.class,
    PrinterTest.class,
    SymbolTableTest.class,

    // equality testing
    EquivalenceTest.class,
    IonEqualsTest.class,

    // hash code tests
    HashCodeCorrectnessTest.class,
    HashCodeDistributionTest.class,

    // General processing test suite
    GoodIonTest.class,
    BadIonTest.class,
    EquivsTest.class,
    RoundTripTest.class,

    // Subclasses of SystemProcessingTestCase are collected to make it
    // easier to run that subset.
    SystemProcessingTests.class,

    // Ditto for WriterTestCase
    IonWriterTests.class,

    IonStreamUtilsTest.class,
    TreeReaderTest.class,
    MiscStreamingTest.class,
    BinaryStreamingTest.class,

    BadIonStreamingTest.class,
    GoodIonStreamingTest.class,
    RoundTripStreamingTest.class,
    ReaderSkippingTest.class,
    ReaderPositioningTest.class,

    IonSystemTest.class,
    ValueFactorySequenceTest.class,
    IonSystemBuilderTest.class
})
public class AllTests
{
}
