/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.ion.system;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.impl._Private_IonBinaryWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Note: because the IonReaderBuilder is used by IonSystem.newReader(...),
 * its build() methods are well-exercised elsewhere. See: ReaderMaker.
 */
public class IonReaderBuilderTest
{

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testMutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        assertSame(catalog, mutable.getCatalog());
        IonReaderBuilder mutableSame = mutable.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutable.getCatalog());
        assertSame(mutable, mutableSame);
    }

    @Test
    public void testImmutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        IonReaderBuilder immutable = mutable.immutable();
        mutable.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutable.getCatalog());
        assertSame(catalog, immutable.getCatalog());
    }

    @Test
    public void testMutatingImmutableFails()
    {
        IonReaderBuilder immutable = IonReaderBuilder.standard().immutable();
        thrown.expect(UnsupportedOperationException.class);
        immutable.setCatalog(new SimpleCatalog());
    }

    @Test
    public void testMutateCopiedImmutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder immutable = IonReaderBuilder.standard().withCatalog(catalog).immutable();
        IonReaderBuilder mutableCopy = immutable.copy();
        assertSame(immutable, immutable.immutable());
        assertNotSame(immutable, mutableCopy);
        assertSame(catalog, mutableCopy.getCatalog());
        mutableCopy.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutableCopy.getCatalog());
    }

    @Test
    public void testMutateCopiedMutable()
    {
        IonCatalog catalog = new SimpleCatalog();
        IonReaderBuilder mutable = IonReaderBuilder.standard().withCatalog(catalog);
        IonReaderBuilder mutableCopy = mutable.copy();
        assertNotSame(mutable, mutable.immutable());
        assertNotSame(mutable, mutableCopy);
        assertSame(mutable, mutable.mutable());
        assertSame(catalog, mutableCopy.getCatalog());
        IonReaderBuilder mutableSame = mutableCopy.withCatalog(new SimpleCatalog());
        assertNotSame(catalog, mutableCopy.getCatalog());
        assertSame(mutableCopy, mutableSame);
    }

    @Test
    public void testSystemFreeRoundtrip() throws IOException
    {
        // No IonSystem in sight.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = _Private_IonBinaryWriterBuilder.standard().build(out);
        writer.writeInt(42);
        writer.finish();
        IonReader reader = IonReaderBuilder.standard().build(out.toByteArray());
        assertEquals(IonType.INT, reader.next());
        assertEquals(42, reader.intValue());
    }

}
