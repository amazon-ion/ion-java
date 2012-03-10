// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import static com.amazon.ion.SystemSymbols.ION_1_0;

import com.amazon.ion.impl._Private_IonSystem;
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
    protected _Private_IonSystem mySystem;
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

    protected _Private_IonSystem system()
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
    protected _Private_IonSystem system(IonCatalog catalog)
    {
        IonSystemBuilder b = IonSystemBuilder.standard().withCatalog(catalog);
        BuilderHack.setBinaryBacked(b, getDomType() == DomType.BACKED);
        IonSystem system = b.build();
        return (_Private_IonSystem) system;
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
    protected static String makeEscapedCharString(char escape)
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


    public static void checkBinaryHeader(byte[] datagram)
    {
        if (datagram.length != 0) // Allow empty "binary" stream
        {
            assertTrue("datagram is too small", datagram.length >= 4);

            assertEquals("datagram cookie byte 1", 0xE0, datagram[0] & 0xff );
            assertEquals("datagram cookie byte 2", 0x01, datagram[1] & 0xff);
            assertEquals("datagram cookie byte 3", 0x00, datagram[2] & 0xff);
            assertEquals("datagram cookie byte 4", 0xEA, datagram[3] & 0xff);
        }
    }


    public static ReaderChecker check(IonReader reader)
    {
        return new ReaderChecker(reader);
    }

    public static IonValueChecker check(IonValue value)
    {
        return new IonValueChecker(value);
    }


    public static void checkType(IonType expected, IonValue actual)
    {
        if (actual.getType() != expected)
        {
            fail("Expected type " + expected + ", found IonValue: " + actual);
        }
    }


    /**
     * Checks that the value is an IonInt with the given value.
     * @param expected may be null to check for null.int
     */
    public static void checkInt(BigInteger expected, IonValue actual)
    {
        checkType(IonType.INT, actual);
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
    public static void checkInt(Long expected, IonValue actual)
    {
        checkType(IonType.INT, actual);
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
    public static void checkInt(Integer expected, IonValue actual)
    {
        checkInt((expected == null ? null : expected.longValue()), actual);
    }


    /**
     * Checks that the value is an IonDecimal with the given value.
     * @param expected may be null to check for null.decimal
     */
    public static void checkDecimal(Double expected, IonValue actual)
    {
        checkType(IonType.DECIMAL, actual);
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
    public static void checkTimestamp(Timestamp expected, IonValue actual)
    {
        checkType(IonType.TIMESTAMP, actual);
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
    public static void checkTimestamp(String expected, IonValue actual)
    {
        checkType(IonType.TIMESTAMP, actual);
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
    public static void checkFloat(Double expected, IonValue actual)
    {
        checkType(IonType.FLOAT, actual);
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

    public static void checkNullNull(IonValue actual)
    {
        checkType(IonType.NULL, actual);
    }


    /**
     * Checks that the value is an IonString with the given text.
     * @param text may be null to check for null.string
     */
    public static void checkString(String text, IonValue value)
    {
        checkType(IonType.STRING, value);
        IonString str = (IonString) value;
        assertEquals("string content", text, str.stringValue());
    }


    /**
     * @param text null means text is unknown
     */
    public static void checkSymbol(String text, int sid, SymbolToken sym)
    {
        assertEquals("SymbolToken.text", text, sym.getText());
        assertEquals("SymbolToken.id",   sid,  sym.getSid());

        if (text != null)
        {
            assertEquals("SymbolToken.assumeText", text, sym.assumeText());
        }
        else
        {
            try
            {
                sym.assumeText();
                fail("expected exception");
            }
            catch (RuntimeException e) { }
        }
    }


    /**
     * Checks that the value is an IonSymbol with the given name.
     * @param name may be null to check for null.symbol
     */
    public static void checkSymbol(String name, IonValue value)
    {
        checkType(IonType.SYMBOL, value);
        IonSymbol sym = (IonSymbol) value;
        assertEquals("symbol name", name, sym.stringValue());
        assertEquals("isNullValue", name == null, sym.isNullValue());

        SymbolToken is = sym.symbolValue();
        if (name == null)
        {
            assertEquals("IonSymbol.symbolValue()", null, is);
        }
        else
        {
            assertEquals("symbolValue.getText()", name, is.getText());
        }
    }

    /**
     * Checks that the value is an IonSymbol with the given name.
     */
    public static void checkSymbol(String name, int id, IonValue value)
    {
        checkType(IonType.SYMBOL, value);
        IonSymbol sym = (IonSymbol) value;

        assertFalse(value.isNullValue());

        if (name == null)
        {
            try {
                sym.stringValue();
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(id, e.getSid());
            }
        }
        else
        {
            assertEquals("symbol name", name, sym.stringValue());
        }

        // just so we can set a break point on this before we lose all context
        int sid = sym.getSymbolId();
        if (sid != id) {
            assertEquals("symbol id", id, sym.getSymbolId());
        }

        checkSymbol(name, id, sym.symbolValue());
    }

    public static void checkUnknownSymbol(int id, IonValue value)
    {
        checkSymbol(null, id, value);
    }


    public static void checkSymbol(String text, int sid, boolean dupe,
                                   SymbolTable symtab)
    {
        assert !dupe || text != null;

        String msg = "text:" + text + " sid:" + sid;

        if (text != null)
        {
            if (sid >= 0)
            {
                assertEquals(msg, text, symtab.findSymbol(sid));
                assertEquals(msg, text, symtab.findKnownSymbol(sid));
            }

            // Can't do this stuff when we have a duplicate symbol.
            if (! dupe)
            {
                assertEquals(msg, sid, symtab.findSymbol(text));
                assertEquals(msg, sid, symtab.addSymbol(text));

                SymbolToken sym = symtab.intern(text);
                assertEquals(msg, sid, sym.getSid());

                sym = symtab.find(text);
                assertEquals(msg, sid, sym.getSid());
            }
        }
        else // No text expected, must have sid
        {
            assertEquals(msg, text, symtab.findKnownSymbol(sid));

            try
            {
                symtab.findSymbol(sid);
                fail("Expected " + UnknownSymbolException.class);
            }
            catch (UnknownSymbolException e)
            {
                assertEquals(sid, e.getSid());
            }
        }
    }

    public static void checkSymbol(String text, int sid, SymbolTable symtab)
    {
        checkSymbol(text, sid, false, symtab);
    }

    public static void checkUnknownSymbol(int sid, SymbolTable symtab)
    {
        checkSymbol(null, sid, false, symtab);
    }

    /**
     * Check that a specific symbol's text isn't known by a symtab.
     * @param text must not be null.
     */
    public static void checkUnknownSymbol(String text, SymbolTable symtab)
    {
        assertEquals(null, symtab.find(text));
        assertEquals(UNKNOWN_SYMBOL_ID, symtab.findSymbol(text));
        if (symtab.isReadOnly())
        {
            try {
                symtab.intern(text);
                fail("Expected exception");
            }
            catch (ReadOnlyValueException e) { }

            try {
                symtab.addSymbol(text);
                fail("Expected exception");
            }
            catch (ReadOnlyValueException e) { }
        }
    }

    /**
     * Check that a specific symbol's text isn't known by a symtab.
     * @param text must not be null.
     * @param sid can be {@link SymbolTable#UNKNOWN_SYMBOL_ID} if not known.
     */
    public static void checkUnknownSymbol(String text, int sid,
                                          SymbolTable symtab)
    {
        checkUnknownSymbol(text, symtab);

        if (sid != UNKNOWN_SYMBOL_ID)
        {
            checkUnknownSymbol(sid, symtab);
        }
    }


    public static void checkAnnotation(String expectedAnnot, IonValue value)
    {
        if (! value.hasTypeAnnotation(expectedAnnot))
        {
            fail("missing annotation " + expectedAnnot
                 + " on IonValue " + value);
        }
    }


    public static void checkLocalTable(SymbolTable symtab)
    {
        assertTrue("table isn't local", symtab.isLocalTable());
        assertFalse("table is shared",  symtab.isSharedTable());
        assertFalse("table is system",  symtab.isSystemTable());
        assertFalse("table is substitute", symtab.isSubstitute());
        assertNotNull("table has imports", symtab.getImportedTables());

        checkUnknownSymbol(" not defined ", UNKNOWN_SYMBOL_ID, symtab);

        SymbolTable system = symtab.getSystemSymbolTable();
        checkSystemTable(system);
        assertEquals(system.getIonVersionId(), symtab.getIonVersionId());
    }

    /**
     * @param symtab must be either system table or empty local table
     */
    public static void checkTrivialLocalTable(SymbolTable symtab)
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

    public static void checkSystemTable(SymbolTable symtab)
    {
        assertFalse(symtab.isLocalTable());
        assertTrue(symtab.isSharedTable());
        assertTrue(symtab.isSystemTable());
        assertFalse("table is substitute", symtab.isSubstitute());
        assertSame(symtab, symtab.getSystemSymbolTable());
        assertEquals(SystemSymbolTable.ION_1_0_MAX_ID, symtab.getMaxId());
        assertEquals(ION_1_0, symtab.getIonVersionId());
    }

    public static SymbolTable findImportedTable(SymbolTable localTable,
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
