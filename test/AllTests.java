/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import software.amazon.ion.*;
import software.amazon.ion.facet.FacetsTest;
import software.amazon.ion.impl.*;
import software.amazon.ion.impl.bin.IonManagedBinaryWriterTest;
import software.amazon.ion.impl.bin.IonRawBinaryWriterTest;
import software.amazon.ion.impl.bin.PooledBlockAllocatorProviderTest;
import software.amazon.ion.impl.bin.WriteBufferTest;
import software.amazon.ion.impl.lite.IonContextTest;
import software.amazon.ion.streaming.*;
import software.amazon.ion.system.IonBinaryWriterBuilderTest;
import software.amazon.ion.system.IonSystemBuilderTest;
import software.amazon.ion.system.IonTextWriterBuilderTest;
import software.amazon.ion.system.SimpleCatalogTest;
import software.amazon.ion.util.EquivalenceTest;
import software.amazon.ion.util.IonStreamUtilsTest;
import software.amazon.ion.util.JarInfoTest;
import software.amazon.ion.util.TextTest;


/**
 * Runs all tests for the Ion project.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // Low-level facilities.
    AssertionsEnabledTest.class,
    IonExceptionTest.class,
    FacetsTest.class,
    TextTest.class,
    JavaNumericsTest.class,
    ExtendedDecimalTest.class,
    IonImplUtilsTest.class,

    // General framework tests
    SimpleCatalogTest.class,

    // Type-based DOM tests
    IonValueTest.class,
    BlobTest.class,
    BoolTest.class,
    ClobTest.class,
    CloneTest.class,
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
    TimestampGoodTest.class,
    TimestampBadTest.class,

    AnnotationEscapesTest.class,
    FieldNameEscapesTest.class,
    StringFieldNameEscapesTest.class,
    SurrogateEscapeTest.class,

    NopPaddingTest.class,

    // Binary format tests
    BinaryTest.class,

    // Utility tests
    JarInfoTest.class,
    LoaderTest.class,
    IterationTest.class,
    SymbolTableTest.class,
    SharedSymbolTableTest.class,
    LocalSymbolTableTest.class,
    IonContextTest.class,

    // Equality tests
    EquivalenceTest.class,
    EquivsTest.class,
    NonEquivsTest.class,
    EquivTimelineTest.class,

    // General processing test suite
    GoodIonTest.class,
    BadIonTest.class,
    RoundTripTest.class,

    // Some tests are collected to make it easier to run interesting subsets.
    SystemProcessingTests.class,
    IonWriterTests.class,
    SpanTests.class,

    IonStreamUtilsTest.class,
    TreeReaderTest.class,
    MiscStreamingTest.class,
    BinaryStreamingTest.class,
    ReaderTest.class,
    InputStreamReaderTest.class,

    BadIonStreamingTest.class,
    GoodIonStreamingTest.class,
    RoundTripStreamingTest.class,
    ReaderDomCopyTest.class,
    ReaderSkippingTest.class,
    ReaderIntegerSizeTest.class,

    IonSystemTest.class,
    ValueFactorySequenceTest.class,
    IonSystemBuilderTest.class,
    IonTextWriterBuilderTest.class,
    IonBinaryWriterBuilderTest.class,
    IonReaderToIonValueTest.class,
    BinaryReaderWrappedValueLengthTest.class,

    // experimental binary writer tests
    PooledBlockAllocatorProviderTest.class,
    WriteBufferTest.class,
    IonRawBinaryWriterTest.class,
    IonManagedBinaryWriterTest.class,

    // Hash code tests
    HashCodeCorrectnessTest.class,
    HashCodeDistributionTest.class,
    HashCodeDeltaCollisionTest.class
})
public class AllTests
{
}
