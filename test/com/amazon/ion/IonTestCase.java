/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import com.amazon.ion.system.StandardIonSystem;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import junit.framework.TestCase;

/**
 * Base class with helpers for Ion unit tests.
 */
public abstract class IonTestCase
    extends TestCase
{
    private static boolean ourSystemPropertiesLoaded = false;
    protected StandardIonSystem mySystem;
    protected IonLoader myLoader;


    public IonTestCase()
    {
    }

    public IonTestCase(String name)
    {
        super(name);
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
     * Gets a {@link File} relative to the <code>testdata</code> tree.
     */
    public static File getTestdataFile(String path)
    {
        String testDataPath =
            requireSystemProperty("com.amazon.iontests.iontestdata.path");
        File testDataDir = new File(testDataPath);
        return new File(testDataDir, path);
    }


    /**
     * @param textFile
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    protected IonDatagram readIonText(File textFile)
        throws Exception
    {
        IonLoader loader = system().newLoader();
        return loader.loadText(textFile);
    }

    protected IonDatagram readIonBinary(File ionFile)
        throws Exception
    {
        IonLoader loader = system().newLoader();
        IonDatagram dg = loader.loadBinary(ionFile);
        dg.deepMaterialize();
        return dg;
    }

    /**
     * Reads the content of an Ion file contained in the test data suite.
     *
     * @param fileName is a path relative to the test data root.
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public IonDatagram readTestFile(String fileName)
        throws Exception
    {
        File file = getTestdataFile(fileName);
        return readIonText(file);
    }


    // ========================================================================
    // Fixture Helpers

    protected StandardIonSystem system()
    {
        if (mySystem == null)
        {
            mySystem = new StandardIonSystem();
        }
        return mySystem;
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


    // ========================================================================
    // Encoding helpers

    public IonDatagram reload(IonDatagram dg)
    {
        byte[] bytes = dg.toBytes();
        checkBinaryHeader(bytes);

        IonDatagram dg1 = loader().load(bytes);
        return dg1;
    }

    public IonValue reload(IonValue value)
    {
        IonDatagram dg = system().newDatagram(value);
        byte[] bytes = dg.toBytes();
        checkBinaryHeader(bytes);

        dg = loader().load(bytes);
        assertEquals(1, dg.size());
        return dg.get(0);
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
        return loader.loadText(text);
    }


    /**
     * Parses text as a single Ion value.  If the text contains more than that,
     * a failure is thrown.
     *
     * @param scanner must not be <code>null</code>.
     * @return a single value, or <code>null</code> if the scanner has nothing
     * on it.
     */
    public IonValue oneValue(IonReader scanner)
    {
        IonValue value = null;

        if (scanner.hasNext())
        {
            value = scanner.next();

            if (scanner.hasNext())
            {
                IonValue part = scanner.next();
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

        Iterator<IonValue> iterator = system().newReader(text);
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
        return system().getLoader().loadText(ionText).toBytes();
    }

    public void assertEscape(char expected, char escapedChar)
    {
        IonString value = (IonString) oneValue(makeEscapedCharString(escapedChar));
        String valString = value.stringValue();
        assertEquals(1, valString.length());
        assertEquals(expected, valString.charAt(0));
    }


    public void checkBinaryHeader(byte[] datagram)
    {
        assertTrue("datagram is too small", datagram.length >= 8);

        long encodedSize =
            (long) (datagram[0] & 0xFF) << 24 |
            (long) (datagram[1] & 0xFF) << 16 |
            (long) (datagram[2] & 0xFF) << 8  |
                   (datagram[3] & 0xFF);

        // TODO check for $FFFFFFFF unknown length
        assertEquals("datagram encoded length", datagram.length, encodedSize);

        assertEquals("datagram cookie byte 1", datagram[4], 0x10);
        assertEquals("datagram cookie byte 2", datagram[5], 0x14);
        assertEquals("datagram cookie byte 3", datagram[6], 0x01);
        assertEquals("datagram cookie byte 4", datagram[7], 0x00);
    }



    /**
     * Checks that the value is an IonInt with the given value.
     * @param expected may be null to check for null.int
     */
    public void checkInt(Long expected, IonValue actual)
    {
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
     * Checks that the value is an IonString with the given text.
     * @param text may be null to check for null.string
     */
    public void checkString(String text, IonValue value)
    {
        IonString str = (IonString) value;
        assertEquals("string content", text, str.stringValue());
    }

    /**
     * Checks that the value is an IonSymbol with the given name.
     * @param name may be null to check for null.symbol
     */
    public void checkSymbol(String name, IonValue value)
    {
        IonSymbol sym = (IonSymbol) value;
        assertEquals("symbol name", name, sym.stringValue());
    }

    /**
     * Checks that the value is an IonSymbol with the given name.
     * @param name shouldn't be null.
     */
    public void checkSymbol(String name, int id, IonValue value)
    {
        IonSymbol sym = (IonSymbol) value;
        assertEquals("symbol name", name, sym.stringValue());
        assertEquals("symbol id", id, sym.intValue());
    }

    public void checkSymbol(String name, int id, SymbolTable symtab)
    {
        assertEquals(id,   symtab.findSymbol(name));
        assertEquals(name, symtab.findSymbol(id));
    }


    public void checkAnnotation(String expectedAnnot, IonValue value)
    {
        boolean foundAnnot = value.hasTypeAnnotation(expectedAnnot);
        assertEquals(foundAnnot, true);
    }


    public void assertIonEquals(IonValue expected, final IonValue found)
    {
        assertSame("element classes", expected.getClass(), found.getClass());

        String[] found_annotations = found.getTypeAnnotations();
        String[] annotations = expected.getTypeAnnotations();
        if (annotations == null || found_annotations == null) {
            assertTrue(annotations == null && found_annotations == null);
        }
        else {
            assertEquals(annotations.length, found_annotations.length);
            for (String s : annotations) {
                checkAnnotation(s, found);
            }
        }

        boolean expectNull = expected.isNullValue();
        assertEquals("isNullValue", expectNull, found.isNullValue());

        if (! expectNull)
        {
            ValueVisitor visitor =
                new ValueVisitor()
                {
                    public void visit(IonBlob expected) throws Exception
                    {
                        assertEquals("blob data",
                                     expected.newBytes(),
                                     ((IonBlob)found).newBytes());
                    }

                    public void visit(IonBool expected) throws Exception
                    {
                        assertEquals("bool value",
                                     expected.booleanValue(),
                                     ((IonBool)found).booleanValue());
                    }

                    public void visit(IonClob expected) throws Exception
                    {
                        assertEquals("clob data",
                                     expected.newBytes(),
                                     ((IonClob)found).newBytes());
                    }

                    /**
                     * NOTE: Datagram equality is currently only based on
                     * user data, not system data.
                     */
                    public void visit(IonDatagram expected) throws Exception
                    {
                        assertEquals("datagram user size",
                                     expected.size(),
                                     ((IonDatagram)found).size());

                        Iterator<IonValue> foundValues =
                            ((IonDatagram)found).iterator();
                        for (IonValue value : expected)
                        {
                            assertIonEquals(value, foundValues.next());
                        }
                    }

                    public void visit(IonDecimal expected) throws Exception
                    {
                        assertEquals("decimal value",
                                     expected.toBigDecimal(),
                                     ((IonDecimal)found).toBigDecimal());
                    }

                    public void visit(IonFloat expected) throws Exception
                    {
                        assertEquals("float value",
                                     expected.toBigDecimal(),
                                     ((IonFloat)found).toBigDecimal());
                    }

                    public void visit(IonInt expected) throws Exception
                    {
                        assertEquals("float value",
                                     expected.toBigInteger(),
                                     ((IonInt)found).toBigInteger());
                    }

                    public void visit(IonList expected) throws Exception
                    {
                        assertEquals("list size",
                                     expected.size(),
                                     ((IonList)found).size());

                        Iterator<IonValue> foundValues =
                            ((IonList)found).iterator();
                        for (IonValue value : expected)
                        {
                            assertIonEquals(value, foundValues.next());
                        }
                    }

                    public void visit(IonNull expected) throws Exception
                    {
                        assertTrue(found instanceof IonNull);
                    }

                    public void visit(IonSexp expected) throws Exception
                    {
                        assertEquals("sexp size",
                                     expected.size(),
                                     ((IonSexp)found).size());

                        Iterator<IonValue> foundValues =
                            ((IonSexp)found).iterator();
                        for (IonValue value : expected)
                        {
                            assertIonEquals(value, foundValues.next());
                        }
                    }

                    public void visit(IonString expected) throws Exception
                    {
                        assertEquals(expected.stringValue(),
                                     ((IonString)found).stringValue());
                    }

                    public void visit(IonStruct value) throws Exception
                    {
                        IonStruct actual = (IonStruct) found;

                        assertEquals(value.size(), actual.size());

                        // TODO Need better struct equality
                    }

                    public void visit(IonSymbol expected) throws Exception
                    {
                        assertEquals(expected.stringValue(),
                                     ((IonSymbol)found).stringValue());
                    }

                    public void visit(IonTimestamp expected) throws Exception
                    {
                        assertEquals(expected.dateValue(),
                                     ((IonTimestamp)found).dateValue());
                        assertEquals(expected.getLocalOffset(),
                                     ((IonTimestamp)found).getLocalOffset());
                    }
                };

            try
            {
                expected.accept(visitor);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }
    }
}
