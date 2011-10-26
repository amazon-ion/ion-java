// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.SystemSymbols.ION_1_0;

import com.amazon.ion.impl.IonSystemImpl;
import com.amazon.ion.impl.IonSystemPrivate;
import com.amazon.ion.junit.Injected;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.junit.IonAssert;
import com.amazon.ion.system.BuilderHack;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.SimpleCatalog;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.runner.RunWith;

/**
 * Base class with helpers for Ion unit tests.
 */
@RunWith(Injected.class)
public abstract class IonTestCase
    extends Assert
{
    protected enum DomType { LITE, BACKED }

    @Inject("domType")
    public static final DomType[] DOM_TYPES = DomType.values();


    public static enum StreamingMode {
        OLD_STREAMING,
        NEW_STREAMING
    }

    @Inject("streamingMode")
    public static final StreamingMode[] STREAMING_DIMENSION =
        //StreamingMode.values();  // ION-180 the old streaming fails numerous regressions.
    { StreamingMode.NEW_STREAMING };



    private static boolean ourSystemPropertiesLoaded = false;
    protected SimpleCatalog    myCatalog;
    protected IonSystemPrivate mySystem;
    protected IonLoader        myLoader;

    //  FIXME: needs java docs
    private DomType domType;
    private StreamingMode desiredStreamingMode;

    public DomType getDomType()
    {
        return domType;
    }

    public void setDomType(DomType type)
    {
        domType = type;
    }


    public void setStreamingMode(StreamingMode mode) {
        desiredStreamingMode = mode;
    }

    public StreamingMode getStreamingMode() {
        return desiredStreamingMode;
    }


    // ========================================================================
    // Access to test data

    public static synchronized void loadSystemProperties()
    {
        if (! ourSystemPropertiesLoaded)
        {
            InputStream stream =
                IonTestCase.class.getResourceAsStream("/system.properties");

            if (stream != null)
            {
                Properties props = new Properties();

                try
                {
                    props.load(stream);

                    Iterator entries = props.entrySet().iterator();
                    while (entries.hasNext())
                    {
                        Map.Entry entry = (Map.Entry) entries.next();

                        String key = (String) entry.getKey();

                        // Command-line values override system.properties
                        if (System.getProperty(key) == null)
                        {
                            System.setProperty(key, (String) entry.getValue());
                        }
                    }
                }
                catch (IOException e)
                {
                    synchronized (System.out)
                    {
                        System.out.println("Caught exception while loading system.properties:");
                        e.printStackTrace(System.out);
                    }
                }
            }

            ourSystemPropertiesLoaded = true;
        }
    }


    public static String requireSystemProperty(String prop)
    {
        loadSystemProperties();

        String value = System.getProperty(prop);
        if (value == null)
        {
            value = System.getenv(prop);
        }
        if (value == null) {
            String message = "Missing required system property: " + prop;
            throw new IllegalStateException(message);
        }
        return value;
    }



    public static File getProjectHome()
    {
        String basedir = System.getProperty("user.dir");
        return new File(basedir);
    }

    public static File getProjectFile(String path)
    {
        return new File(getProjectHome(), path);
    }

    /**
     * Gets a {@link File} contained in the test data suite.
     *
     * @param path is relative to the {@code testdata} directory.
     */
    public static File getTestdataFile(String path)
    {
        String testDataPath =
            requireSystemProperty("com.amazon.iontests.iontestdata.path");
        File testDataDir = new File(testDataPath);
        return new File(testDataDir, path);
    }

    /**
     * Reads the content of an Ion file contained in the test data suite.
     *
     * @param path is relative to the {@code testdata} directory.
     *
     * @throws FileNotFoundException
     * @throws IOException
     */
    public IonDatagram loadTestFile(String path)
        throws IOException
    {
        File file = getTestdataFile(path);
        return load(file);
    }

    public IonDatagram load(File ionFile)
        throws IonException, IOException
    {
        IonLoader loader = loader();
        IonDatagram dg = loader.load(ionFile);

        // Flush out any encoding problems in the data.
        forceMaterialization(dg);

        return dg;
    }


    @SuppressWarnings("deprecation")
    public void forceMaterialization(IonValue value)
    {
        value.deepMaterialize();
    }

    // ========================================================================
    // Fixture Helpers

    protected IonSystemPrivate system()
    {
        if (mySystem == null)
        {
            mySystem = system(myCatalog);
        }
        return mySystem;
    }

    // added helper, this returns a separate system
    // every time since the user is passing in a catalog
    // which changes the state of the system object
    protected IonSystemPrivate system(IonCatalog catalog)
    {
        IonSystemBuilder b = IonSystemBuilder.standard().withCatalog(catalog);
        BuilderHack.setBinaryBacked(b, getDomType() == DomType.BACKED);
        IonSystem system = b.build();

        if (system instanceof IonSystemImpl)
        {
            // TODO we really should phase this out...
            ((IonSystemImpl) system).useNewReaders_UNSUPPORTED_MAGIC =
                (getStreamingMode() == StreamingMode.NEW_STREAMING);
        }
        return (IonSystemPrivate) system;
    }

    protected SimpleCatalog catalog()
    {
        if (myCatalog == null)
        {
            myCatalog = (SimpleCatalog) system().getCatalog();
        }
        return myCatalog;
    }

    protected IonLoader loader()
    {
        if (myLoader == null)
        {
            myLoader = system().getLoader();
        }
        return myLoader;
    }


    /**
     * Constructs an Ion-encoded string literal with a single escaped character.
     * For example, <code>makeEscapedCharString('n')</code> returns
     * (Java) <code>"\"\\n\""</code>, equivalent to Ion <code>"\n"</code>.
     */
    protected String makeEscapedCharString(char escape)
    {
        final String QT = "\"";
        final String BS = "\\";

        String result = QT + BS + escape + QT;
        return result;
    }

    protected int systemMaxId()
    {
        return system().getSystemSymbolTable().getMaxId();
    }


    // ========================================================================
    // Encoding helpers

    /**
     * Gets bytes of datagram and loads into a new one.
     */
    public IonDatagram reload(IonDatagram dg)
    {
        byte[] bytes = dg.getBytes();
        checkBinaryHeader(bytes);

        IonDatagram dg1 = loader().load(bytes);
        return dg1;
    }

    /**
     * Put value into a datagram, get bytes, and reload to single value.
     * Result should be equivalent.
     */
    public IonValue reload(IonValue value)
    {
        IonDatagram dg = system().newDatagram(value);
        byte[] bytes = dg.getBytes();
        checkBinaryHeader(bytes);

        try {
            dg = loader().load(bytes);
        } catch (final IonException e) {
            final String hex = BinaryTest.bytesToHex(bytes);
            throw new IonException("Bad bytes: " + hex, e);
        }
        assertEquals(1, dg.size());
        return dg.get(0);
    }



    /**
     * Materializes a shared symtab using
     * {@link IonSystem#newSharedSymbolTable(IonReader)}.
     * No catalog registration is performed.
     */
    public SymbolTable loadSharedSymtab(String serializedSymbolTable)
    {
        IonReader reader = system().newReader(serializedSymbolTable);
        SymbolTable shared = system().newSharedSymbolTable(reader);
        assertTrue(shared.isSharedTable());
        return shared;
    }

    /**
     * Materializes a shared symtab and registeres it in the default
     * catalog.
     *
     * @see #catalog()
     */
    public SymbolTable registerSharedSymtab(String serializedSymbolTable)
    {
        SymbolTable shared = loadSharedSymtab(serializedSymbolTable);
        catalog().putTable(shared);
        return shared;
    }



    // ========================================================================
    // Processing Ion text

    /**
     * Parses text as a sequence of Ion values.
     *
     * @param text must not be <code>null</code>.
     * @return the list of values in <code>text</code>, not <code>null</code>.
     */
    public IonDatagram values(String text)
        throws Exception
    {
        IonLoader loader = system().newLoader();
        return loader.load(text);
    }


    /**
     * Parses text as a single Ion value.  If the text contains more than that,
     * a failure is thrown.
     *
     * @param iterator must not be <code>null</code>.
     * @return a single value, or <code>null</code> if the scanner has nothing
     * on it.
     */
    public IonValue singleValue(Iterator<IonValue> iterator)
    {
        IonValue value = null;

        if (iterator.hasNext())
        {
            value = iterator.next();

            if (iterator.hasNext())
            {
                IonValue part = iterator.next();
                fail("Found unexpected part: " + part);
            }
        }

        return value;
    }



    /**
     * Parses text as a single Ion value.  If the text contains more than that,
     * a failure is thrown.
     *
     * @param text must not be <code>null</code>.
     * @return a single value, not <code>null</code>.
     */
    public IonValue oneValue(String text)
    {
        IonValue value = null;

        Iterator<IonValue> iterator = system().iterate(text);
        if (iterator.hasNext())
        {
            value = iterator.next();

            if (iterator.hasNext())
            {
                IonValue part = iterator.next();
                fail("Found unexpected part <" + part + "> in text: " + text);
            }
        }
        else
        {
            fail("No data found in text: " + text);
        }

        return value;
    }


    public IonDecimal decimal(String text)
    {
        return (IonDecimal) oneValue(text);
    }

    public IonSexp oneSexp(String text)
    {
        return (IonSexp) oneValue(text);
    }

    public IonStruct struct(String text)
    {
        return (IonStruct) oneValue(text);
    }


    public void putParsedValue(IonStruct struct,
                               String fieldName,
                               String singleValue)
    {
        IonValue v = null;
        if (singleValue != null)
        {
            IonSystem system = struct.getSystem();
            v = system.singleValue(singleValue);
        }
        struct.put(fieldName, v);
    }


    /**
     * Parse a broken value, expecting an IonException.
     * @param text can be anything.
     */
    public void badValue(String text)
    {
        try
        {
            oneValue(text);
            fail("Expected IonException");
        }
        catch (IonException e)
        {
            // TODO use a tighter exception type.  SyntaxError?
            // This line is here solely to allow a breakpoint to be set.
            @SuppressWarnings("unused")
            String msg = e.getMessage();
        }
    }


    // TODO add to IonSystem()?
    public byte[] encode(String ionText)
    {
        IonDatagram dg = loader().load(ionText);  // Keep here for breakpoint
        return dg.getBytes();
    }


    public void assertEscape(char expected, char escapedChar)
    {
        String ionText = makeEscapedCharString(escapedChar);
        IonString value = (IonString) oneValue(ionText);
        String valString = value.stringValue();
        assertEquals(1, valString.length());
        assertEquals(expected, valString.charAt(0));
    }


    public void checkBinaryHeader(byte[] datagram)
    {
        assertTrue("datagram is too small", datagram.length >= 4);

        assertEquals("datagram cookie byte 1", 0xE0, datagram[0] & 0xff );
        assertEquals("datagram cookie byte 2", 0x01, datagram[1] & 0xff);
        assertEquals("datagram cookie byte 3", 0x00, datagram[2] & 0xff);
        assertEquals("datagram cookie byte 4", 0xEA, datagram[3] & 0xff);
    }



    /**
     * Checks that the value is an IonInt with the given value.
     * @param expected may be null to check for null.int
     */
    public void checkInt(BigInteger expected, IonValue actual)
    {
        assertSame(IonType.INT, actual.getType());
        IonInt i = (IonInt) actual;

        if (expected == null) {
            assertTrue("expected null value", actual.isNullValue());
        }
        else
        {
            assertEquals("int content", expected, i.bigIntegerValue());
        }
    }

    /**
     * Checks that the value is an IonInt with the given value.
     * @param expected may be null to check for null.int
     */
    public void checkInt(Long expected, IonValue actual)
    {
        assertSame(IonType.INT, actual.getType());
        IonInt i = (IonInt) actual;

        if (expected == null) {
            assertTrue("expected null value", actual.isNullValue());
        }
        else
        {
            assertEquals("int content", expected.longValue(), i.longValue());
        }
    }

    /**
     * Checks that the value is an IonInt with the given value.
     * @param expected may be null to check for null.int
     */
    public void checkInt(Integer expected, IonValue actual)
    {
        checkInt((expected == null ? null : expected.longValue()), actual);
    }


    /**
     * Checks that the value is an IonDecimal with the given value.
     * @param expected may be null to check for null.decimal
     */
    public void checkDecimal(Double expected, IonValue actual)
    {
        assertSame(IonType.DECIMAL, actual.getType());
        IonDecimal i = (IonDecimal) actual;

        if (expected == null) {
            assertTrue("expected null value", actual.isNullValue());
        }
        else
        {
            assertEquals("decimal content",
                         expected.doubleValue(), i.doubleValue(), 0d);
        }
    }


    /**
     * Checks that the value is an IonTimestamp with the given value.
     * @param expected may be null to check for null.timestamp
     */
    public void checkTimestamp(Timestamp expected, IonValue actual)
    {
        assertSame(IonType.TIMESTAMP, actual.getType());
        IonTimestamp v = (IonTimestamp) actual;

        Timestamp actualTime = v.timestampValue();

        if (expected == null) {
            assertTrue("expected null value", v.isNullValue());
            assertNull(actualTime);
        }
        else
        {
            assertEquals("timestamp", expected, actualTime);
            assertEquals("timestamp content",
                         expected.toString(), actualTime.toString());
        }
    }

    /**
     * Checks that the value is an IonTimestamp with the given value.
     * @param expected may be null to check for null.timestamp
     */
    public void checkTimestamp(String expected, IonValue actual)
    {
        assertSame(IonType.TIMESTAMP, actual.getType());
        IonTimestamp v = (IonTimestamp) actual;

        if (expected == null) {
            assertTrue("expected null value", v.isNullValue());
            assertNull(v.timestampValue());
        }
        else
        {
            assertEquals("timestamp content",
                         expected, v.timestampValue().toString());
            assertEquals("timestamp content",
                         expected, v.toString());
        }
    }


    /**
     * Checks that the value is an IonFloat with the given value.
     * @param expected may be null to check for null.float
     */
    public void checkFloat(Double expected, IonValue actual)
    {
        assertSame(IonType.FLOAT, actual.getType());
        IonFloat i = (IonFloat) actual;

        if (expected == null) {
            assertTrue("expected null value", actual.isNullValue());
        }
        else
        {
            assertEquals("decimal content",
                         expected.doubleValue(), i.doubleValue(), 0d);
        }
    }



    public void checkNullNull(IonValue actual)
    {
        assertSame(IonType.NULL, actual.getType());
        IonNull n = (IonNull) actual;
        assertNotNull(n);
    }


    /**
     * Checks that the value is an IonString with the given text.
     * @param text may be null to check for null.string
     */
    public void checkString(String text, IonValue value)
    {
        assertSame(IonType.STRING, value.getType());
        IonString str = (IonString) value;
        assertEquals("string content", text, str.stringValue());
    }

    /**
     * Checks that the value is an IonSymbol with the given name.
     * @param name may be null to check for null.symbol
     */
    public void checkSymbol(String name, IonValue value)
    {
        assertSame(IonType.SYMBOL, value.getType());
        IonSymbol sym = (IonSymbol) value;
        assertEquals("symbol name", name, sym.stringValue());
    }

    /**
     * Checks that the value is an IonSymbol with the given name.
     * @param name shouldn't be null.
     */
    public void checkSymbol(String name, int id, IonValue value)
    {
        assertSame(IonType.SYMBOL, value.getType());
        IonSymbol sym = (IonSymbol) value;
        assertEquals("symbol name", name, sym.stringValue());
        // just so we can set a break point on this before we lose all context
        int sid = sym.getSymbolId();
        if (sid != id) {
            assertEquals("symbol id", id, sym.getSymbolId());
        }
    }

    public void checkSymbol(String name, int id, SymbolTable symtab)
    {
        assertEquals(id,   symtab.findSymbol(name));
        assertEquals(name, symtab.findSymbol(id));
    }


    public static void checkAnnotation(String expectedAnnot, IonValue value)
    {
        assertTrue("missing annotation",
                   value.hasTypeAnnotation(expectedAnnot));
    }


    public void checkLocalTable(SymbolTable symtab)
    {
        assertTrue("table isn't local", symtab.isLocalTable());
        assertFalse("table is shared",  symtab.isSharedTable());
        assertFalse("table is system",  symtab.isSystemTable());
        assertNotNull("table has imports", symtab.getImportedTables());

        SymbolTable system = symtab.getSystemSymbolTable();
        checkSystemTable(system);
        assertEquals(system.getIonVersionId(), symtab.getIonVersionId());
    }

    /**
     * @param symtab must be either system table or empty local table
     */
    public void checkTrivialLocalTable(SymbolTable symtab)
    {
        SymbolTable system = symtab.getSystemSymbolTable();

        if (symtab.isLocalTable())
        {
            checkLocalTable(symtab);
            assertEquals(system.getMaxId(), symtab.getMaxId());
        }
        else
        {
            assertSame(symtab, system);
            checkSystemTable(symtab);
        }
    }

    public void checkSystemTable(SymbolTable symtab)
    {
        assertFalse(symtab.isLocalTable());
        assertTrue(symtab.isSharedTable());
        assertTrue(symtab.isSystemTable());
        assertSame(symtab, symtab.getSystemSymbolTable());
        assertEquals(SystemSymbolTable.ION_1_0_MAX_ID, symtab.getMaxId());
        assertEquals(ION_1_0, symtab.getIonVersionId());
    }

    public SymbolTable findImportedTable(SymbolTable localTable,
                                         String importName)
    {
        SymbolTable[] imports = localTable.getImportedTables();
        if (imports == null) return null;

        for (int i = 0; i < imports.length; i++)
        {
            SymbolTable current = imports[i];
            // FIXME why does this allow null?
            if (current != null && importName.equals(current.getName()))
            {
                return current;
            }
        }

        return null;
    }

    public static void assertPreciselyEquals(Decimal expected,
                                             Decimal actual)
    {
        assertEquals(expected, actual);
        assertEquals("value",
                     expected.unscaledValue(), actual.unscaledValue());
        assertEquals("scale",
                     expected.scale(), actual.scale());
        assertEquals("isNegativeZero",
                     expected.isNegativeZero(), actual.isNegativeZero());
    }


    public static void assertEquals(IonValue expected, IonValue actual)
    {
        IonAssert.assertIonEquals(expected, actual);
    }


    public void assertEqualBytes(byte[] expected, int start, int limit,
                                 byte[] actual)
    {
        for (int i = start; i < limit; i++)
        {
            assertEquals(expected[i], actual[i - start]);
        }
    }

    /**
     * Tests that some data parses, clones, and prints back to the same text.
     * @param input  Ion text data
     */
    public void testSimpleClone(String input)
    {
        IonValue data = system().singleValue(input);
        IonValue clone = data.clone();
        assertEquals(input, clone.toString());
        assertEquals(data, clone);
    }

    public void logSkippedTest()
    {
        System.err.println("WARNING: skipped a test in " + getClass().getName());
    }

    /** Temporary bridge from JUnit 3 */
    public void setUp() throws Exception { }

    /** Temporary bridge from JUnit 3 */
    public void tearDown() throws Exception { }

    /** JUnit 4 disables this */
    public static void assertEquals(double expected, double actual)
    {
        assertEquals(expected, actual, 0d);
    }

    /** JUnit 4 disables this */
    public static void assertEquals(String message, double expected, double actual)
    {
        assertEquals(message, expected, actual, 0d);
    }
}
