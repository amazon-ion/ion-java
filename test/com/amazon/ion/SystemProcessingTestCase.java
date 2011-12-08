// Copyright (c) 2008-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.Symtabs.LocalSymbolTablePrefix;
import static com.amazon.ion.SystemSymbols.ION_1_0;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.ION_SHARED_SYMBOL_TABLE_SID;
import static com.amazon.ion.TestUtils.FERMATA;

import com.amazon.ion.impl.IonImplUtils;
import com.amazon.ion.impl.IonReaderTextRawTokensX;
import com.amazon.ion.impl.IonUTF8;
import com.amazon.ion.impl.SymbolTableTest;
import com.amazon.ion.system.SimpleCatalog;
import org.junit.Ignore;
import org.junit.Test;



/**
 *
 */
public abstract class SystemProcessingTestCase
    extends IonTestCase
{
    // so far 1000 to 10000 (by 1000's) assigned Dec 31, 2009
    // as classid's
    String getDebugClassId() {
        return this.getClass().getSimpleName();
    }
    static String current_test = null;
    static String current_class = null;
    static String getCurrentTestAndClass() {
        String s = " \""+current_class+"\", \""+current_test+"\" ";
        return s;
    }
    public final void startTestCheckpoint(String testid)
    {
        String classid = getDebugClassId();

// FIXME: set these to null so we don't stop or print anything
        String interesting_classid = null;
        String interesting_testid = null;


        current_test = testid;
        current_class = classid;

        if (testid.equals(interesting_testid) && classid.equals(interesting_classid)) {
            System.out.println("Interesting test encountered.");
            System.out.println("\tClass: "+classid);
            System.out.println("\tTestCase: "+testid);
            System.out.println("\t(this message from SystemProcessingTestCase.startTestCheckpoint(), approx line 50)");
            System.out.println();
            return;
        }
        return;
    }

    protected abstract void prepare(String text)
        throws Exception;

    protected abstract void startIteration()
        throws Exception;

    protected final void startIteration(String text)
        throws Exception
    {
        prepare(text);
        startIteration();
    }

    protected abstract void startSystemIteration()
        throws Exception;

    protected abstract void nextValue()
        throws Exception;

    protected abstract void stepIn()
        throws Exception;

    protected abstract void stepOut()
        throws Exception;

    protected abstract IonType currentValueType()
        throws Exception;

    abstract SymbolTable currentSymtab();


    /**
     * Checks a field name that's missing from the context symbol table,
     * generally because there was no exact match to an import.
     *
     * @returns true when the local sid was matched
     */
    abstract boolean checkMissingFieldName(String expectedText,
                                           int expectedEncodedSid,
                                           int expectedLocalSid)
        throws Exception;


    protected abstract void checkAnnotation(String expected, int expectedSid)
        throws Exception;

    /** Check that all the annotations exist in the given order. */
    protected abstract void checkAnnotations(String[] expecteds,
                                             int[] expectedSids)
        throws Exception;

    protected abstract void checkType(IonType expected)
        throws Exception;

    protected abstract void checkString(String expected)
        throws Exception;

    protected abstract void checkSymbol(String expected)
        throws Exception;

    protected abstract void checkSymbol(String expected, int expectedSid)
        throws Exception;

    /**
     * Checks a symbol that's missing from the context symbol table,
     * generally because there was no exact match to an import.
     *
     * @returns true when the local sid was matched
     */
    protected abstract boolean checkMissingSymbol(String expected,
                                                  int expectedSymbolTableSid,
                                                  int expectedLocalSid)
        throws Exception;

    protected abstract void checkInt(long expected)
        throws Exception;

    protected abstract void checkDecimal(double expected)
        throws Exception;

    protected abstract void checkFloat(double expected)
        throws Exception;

    /**
     * @param expected is the canonical form of the timestamp
     */
    protected abstract void checkTimestamp(Timestamp expected)
        throws Exception;

    /**
     * @param expected is the canonical form of the timestamp
     */
    protected void checkTimestamp(String expected)
        throws Exception
    {
        checkTimestamp(Timestamp.valueOf(expected));
    }

    protected abstract void checkEof()
        throws Exception;


    //=========================================================================

    /**
     * TODO how is this different from {@link IonImplUtils#utf8(String)}?
     */
    public static byte[] convertUtf16UnitsToUtf8(String text)
    {
        byte[] data = new byte[4*text.length()];
        int limit = 0;
        for (int i = 0; i < text.length(); i++)
        {
            char c = text.charAt(i);
            limit += IonUTF8.convertToUTF8Bytes(c, data, limit, data.length - limit);
        }

        byte[] result = new byte[limit];
        System.arraycopy(data, 0, result, 0, limit);
        return result;
    }


    //=========================================================================

    /** TODO ION-165 This is broken for loaders which are now more lazy */
    @Test @Ignore
    public void testLocalTableResetting()
        throws Exception
    {
        startTestCheckpoint("testLocalTableResetting");

        String text = "bar foo $ion_1_0 1 far boo";

        prepare(text);
        startIteration();

        nextValue();
        checkSymbol("bar");

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo");
        SymbolTable current = currentSymtab();
        assertSame(table1, current);

        // The symbol table changes here ...
        nextValue();

        // FIXME --- how should this work?
        if (!IonType.INT.equals(currentValueType())) {
            // if we didn't hit the int 1 then we should have the $ion_1_0
            checkSymbol(ION_1_0, ION_1_0_SID);
            nextValue();
            // ???
        }
        checkInt(1);

        // we should have reset to the system symbol table here
        SymbolTable table3 = currentSymtab();
        assertNotSame(table1, table3);
        // nope, this may be the next local that will hold 'far' and 'boo':
        // assertTrue("the reset table should be a trivial table (system or null)", UnifiedSymbolTable.isTrivialTable(table3));

        nextValue();
        checkSymbol("far");

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);

// here for debug
if (table1 == table2) {
    System.out.println();
    System.out.println("in class: "+this.getDebugClassId());
    System.out.println("in test: "+"testLocalTableResetting");
    System.out.println("the current and the initial symbol tables are the same, they should be different");
    System.out.println(this.getClass().getCanonicalName()+ " about line 181");
    System.out.println();
}
        assertNotSame(table1, table2);

        nextValue();
        checkSymbol("boo");
        SymbolTable table4 = currentSymtab();
        assertSame(table2, table4);
    }

    @Test
    public void testTrivialLocalTableResetting()
        throws Exception
    {
        startTestCheckpoint("testTrivialLocalTableResetting");

        String text = "1 $ion_1_0 2";

        startIteration(text);

        nextValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkTrivialLocalTable(table1);

        // move from the 1 to either the $ion_1_0 or the 2
        nextValue();

// FIXME --- how should this work?
//           since there is no symbol table other than
//           the default system the $ion_1_0 in the middle
//           of this sequence isn't meaningful, but if
//           you're asking for system values with a system
//           reader you will see it.  If you convert through
//           a number of readers and writers you might or
//           might not preserve the $ion_1_0
        if (!IonType.INT.equals(currentValueType())) {
            // if we didn't hit the int 2 then we should have the $ion_1_0
            checkSymbol(ION_1_0, ION_1_0_SID);
            nextValue();
        }

        checkInt(2);

        SymbolTable table2 = currentSymtab();
        checkTrivialLocalTable(table2);
        if (table1.isLocalTable() || table2.isLocalTable())
        {
            assertNotSame(table1, table2);
        }
        assertEquals(systemMaxId(), table2.getMaxId());
    }

    @Test
    public void testLocalTableReplacement()
        throws Exception
    {
        startTestCheckpoint("testLocalTableReplacement");

        String text =
            "$ion_symbol_table::{" +
            "  symbols:[ \"foo\", \"bar\" ]," +
            "}\n" +
            "bar foo\n" +
            "$ion_symbol_table::{" +
            "  symbols:[ \"bar\" ]," +
            "}\n" +
            "bar foo";

        startIteration(text);

        nextValue();
        checkSymbol("bar", systemMaxId() + 2);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkSymbol("foo", systemMaxId() + 1);

        // Symtab changes here...

        nextValue();
        checkSymbol("bar", systemMaxId() + 1);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertTrue(systemMaxId() + 1 <= table2.getMaxId());
        assertTrue(systemMaxId() + 2 >= table2.getMaxId());

        nextValue();
        checkSymbol("foo");
        assertSame(table2, currentSymtab());
    }

    @Test
    public void testTrivialLocalTableReplacement()
        throws Exception
    {
        startTestCheckpoint("testTrivialLocalTableReplacement");

        String text =
            "$ion_symbol_table::{" +
            "}\n" +
            "1\n" +
            "$ion_symbol_table::{" +
            "}\n" +
            "2";

        startIteration(text);

        nextValue();
        checkInt(1);

        SymbolTable table1 = currentSymtab();
        checkLocalTable(table1);

        nextValue();
        checkInt(2);

        SymbolTable table2 = currentSymtab();
        checkLocalTable(table2);
        assertNotSame(table1, table2);
        assertEquals(systemMaxId(), table2.getMaxId());
    }


    @Test
    public void testLocalSymtabWithOpenContent()
        throws Exception
    {
        startTestCheckpoint("testLocalSymtabWithOpenContent");

        String data = "$ion_symbol_table::{open:33,symbols:[\"a\",\"b\"]} b";

        startIteration(data);
        nextValue();
        checkSymbol("b", 11);
    }


    /**
     * Import v2 but catalog has v1.
     */
    @Test
    public void testLocalTableWithLesserImport()
        throws Exception
    {
        startTestCheckpoint("testLocalTableWithLesserImport");

        final int fred1id = systemMaxId() + 1;
        final int fred2id = systemMaxId() + 2;
        final int fred3id = systemMaxId() + 3;

        final int local = systemMaxId() + Symtabs.FRED_MAX_IDS[2];
        final int local3id = local + 3;

        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        Symtabs.register("fred", 1, catalog);
        Symtabs.register("fred", 2, catalog);

        // {  name:"fred", version:1,
        //    symbols:["fred_1", "fred_2"]}

        // {  name:"fred", version:2,
        //    symbols:["fred_1","fred_2","fred_3","fred_4",]}

        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"fred\", version:2, " +
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]," +
            "}\n" +
            "local1 local2 fred_1 fred_2 fred_3 $12 $99 [{fred_3:local2, $98:$97}]";
        // Nesting flushed out a bug at one point


        prepare(text);

        // Remove the imported table: fred version 2
        // We'll read using fred version 1 with a max id of 2
        // which causes fred_3 to "disappear".
        // If forced to, the reader will assign fred_2 to a new local id.
        assertNotNull(catalog.removeTable("fred", 2));

        // at this point the effective symbol list should be:
        // fred::version::'2'::symbols:[
        // 10::   '''fred_1''',
        // 11::   '''fred_2''',
        // 12::        null,
        // 13::        null,
        // local::symbols:[
        // 14::   '''local1''',
        // 15::   '''local2''',
        // 16::   '''fred_3''',
        // 17::   '''fred_4''',

        startIteration();

        nextValue();
        checkSymbol("local1");

        nextValue();
        checkSymbol("local2");

        nextValue();
        checkSymbol("fred_1", fred1id);

        nextValue();
        checkSymbol("fred_2", fred2id);

        nextValue();
        // it doesn't matter if fred 2 is local or not,
        // fred 3 should be in the shared symbol table
        boolean is_fred3_a_local_symbol =
            checkMissingSymbol("fred_3", fred3id, local3id);

        nextValue();
// TODO checkAbsentSidLiteral("fred_3", fred3id);

        nextValue();
        checkSymbol(null, 99);

        nextValue();
        stepIn();
            nextValue();
            stepIn();
                nextValue();
                checkMissingFieldName("fred_3", fred3id, local3id);
                checkSymbol("local2");

                nextValue();
                checkMissingFieldName(null, 98, 98);
                checkSymbol(null, 97);

                checkEof();
            stepOut();
            checkEof();
        stepOut();

        checkEof();

        if (is_fred3_a_local_symbol) return; // force is_fred2_a_local_symbol to be used
    }

    /**
     * Import v2 but catalog has v3.
     */
    @Test
    public void testLocalTableWithGreaterImport()
        throws Exception
    {
        startTestCheckpoint("testLocalTableWithGreaterImport");

        final int fred1id_symtab = systemMaxId() + 1;  // expect 9 + 1 = 10
        final int fred2id_symtab = systemMaxId() + 2;
        final int fred3id_symtab = systemMaxId() + 3;

        final int local = systemMaxId() + Symtabs.FRED_MAX_IDS[2];
        final int local1id = local + 1; // expect 9 + 4 + 1 = 14 id for local1
        final int local2id = local + 2; // 15: id for local2
        final int local3id = local + 3; // 16: id for fred_2, which has been removed from version 2 of the sym tab, so is now local
        final int local4id = local + 4; // 17: id for fred_5, which isn't present when version 2 was defined

        SimpleCatalog catalog = (SimpleCatalog) system().getCatalog();
        Symtabs.register("fred", 1, catalog);
        Symtabs.register("fred", 2, catalog);
        SymbolTable fredV3 = Symtabs.register("fred", 3, catalog);

        // Make sure our syms don't overlap.
        assertTrue(fredV3.findSymbol("fred_5") != local3id);

        // version: 1
        // {  name:"fred", version:1,
        // symbols:["fred_1", "fred_2"]}

        // version: 3: /* Removed fred_2 */
        //"{  name:"fred", version:3," +
        //"  symbols:["fred_1",null,"fred_3","fred_4","fred_5",]}"



        // fred_5 is not in table version 2, so it gets local symbol
        // fred_2 is missing from version 3
        String text =
            LocalSymbolTablePrefix +
            "{" +
            "  imports:[{name:\"fred\", version:2, " +
            "            max_id:" + Symtabs.FRED_MAX_IDS[2] + "}]" +
            "} " +
            "local1 local2 fred_1 fred_2 fred_3 fred_5";

        // at this point the effective symbol list should be:
        // fred::version::'2'::symbols:[
        // 10::   '''fred_1''',
        // 11::        null, // removed when fred version 2 was removed and replaced by fred version 3 (which has the symbol fred_2 removed)
        // 12::   '''fred_3''',
        // 13::   '''fred_4''',
        // local::symbols:[
        // 14::   '''local1''',
        // 15::   '''local2''',
        // 16::   '''fred_2''', // since it's been removed from fred 3 (local 3)
        // 17::   '''fred_5''', // since it's below the max is of fred 2 so not visible (local 4)

        prepare(text);

        // Remove the imported table before decoding the binary.
        assertNotNull(catalog.removeTable("fred", 2));

        startIteration();

        nextValue();
        checkSymbol("local1");

        nextValue();
        checkSymbol("local2");

        nextValue();
        checkSymbol("fred_1", fred1id_symtab);

        nextValue();
        checkMissingSymbol("fred_2", fred2id_symtab, local3id);

        nextValue();
        checkSymbol("fred_3", fred3id_symtab);

        nextValue();
        checkSymbol("fred_5");

        checkEof();
    }


    @Test
    public void testSharedTableNotAddedToCatalog()
        throws Exception
    {
        startTestCheckpoint("testSharedTableNotAddedToCatalog");

        String text =
            ION_1_0 + " " +
            SymbolTableTest.IMPORTED_1_SERIALIZED +
            " 'imported 2'";
        assertNull(system().getCatalog().getTable("imported"));

        startIteration(text);
        try {
            nextValue();
        }
        catch (IonReaderTextRawTokensX.IonReaderTextTokenException e) {
            // FIXME what the heck?
            testSharedTableNotAddedToCatalog();
        }
        checkType(IonType.STRUCT);
        checkAnnotation(ION_SHARED_SYMBOL_TABLE,
                        ION_SHARED_SYMBOL_TABLE_SID);

        assertNull(system().getCatalog().getTable("imported"));

        nextValue();
        checkSymbol("imported 2");
    }


    /**
     * Parse Ion string data and ensure it matches expected text.
     */
    protected void testString(String expectedValue, String ionData)
        throws Exception
    {
        testString(expectedValue, ionData, ionData);
    }

    /**
     * Parse Ion string data and ensure it matches expected text.
     */
    protected void testString(String expectedValue,
                              String expectedRendering,
                              String ionData)
        throws Exception
    {
        startIteration(ionData);
        nextValue();
        checkString(expectedValue);
        checkEof();
    }

    @Test
    public void testUnicodeCharacters()
        throws Exception
    {
        startTestCheckpoint("testUnicodeCharacters");

        String ionData = "\"\\0\"";
        testString("\0", ionData);

        ionData = "\"\\x01\"";
        testString("\01", ionData);

        // U+007F is a control character
        ionData = "\"\\x7f\"";
        testString("\u007f", ionData);

        ionData = "\"\\xff\"";
        testString("\u00ff", ionData);

        ionData = "\"\\" + "u0110\""; // Carefully avoid Java escape
        testString("\u0110", ionData);

        ionData = "\"\\" + "uffff\""; // Carefully avoid Java escape
        testString("\uffff", ionData);

        ionData = "\"\\" + "U0001d110\""; // Carefully avoid Java escape
        testString("\ud834\udd10", ionData);

        // The largest legal code point
        ionData = "\"\\" + "U0010ffff\""; // Carefully avoid Java escape
        testString("\udbff\udfff", ionData);
    }


    @Test
    public void testSurrogateGluing()
        throws Exception
    {
        startTestCheckpoint("testSurrogateGluing");

        // Three ways to represent each surrogate:
        //  1) The actual UTF-8 or UTF-16
        //  2) The \ u 2-byte escape
        //  3) The \ U 4-byte escape
        String[] highs = { "\uD834", "\\" + "uD834", "\\" + "U0000D834" };
        String[] lows  = { "\uDD10", "\\" + "uDD10", "\\" + "U0000DD10" };

        for (int i = 0; i < highs.length; i++) {
            String high = highs[i];
            for (int j = 0; j < lows.length; j++) {
                String low = lows[j];

                String ionData = '"' + high + low + '"';
                testString(FERMATA, "\"\\U0001d110\"", ionData);

                ionData = "'''" + high + low + "'''";
                testString(FERMATA, "\"\\U0001d110\"", ionData);

                ionData = "'''" + high + "''' '''" + low + "'''";
                testString(FERMATA, "\"\\U0001d110\"", ionData);
            }
        }
    }

    @Test
    public void testQuotesInLongStrings()
        throws Exception
    {
        startTestCheckpoint("testQuotesInLongStrings");

        testString("'", "\"'\"", "'''\\''''");
        testString("x''y", "\"x''y\"", "'''x''y'''");
        testString("x'''y", "\"x'''y\"", "'''x''\\'y'''");
        testString("x\"y", "\"x\\\"y\"", "'''x\"y'''");
        testString("x\"\"y", "\"x\\\"\\\"y\"", "'''x\"\"y'''");
    }

    // TODO similar tests on clob

    @Test @Ignore
    public void testPosInt() // TODO rework?
        throws Exception
    {
        startTestCheckpoint("XXXtestPosInt");

        startIteration("+1");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkInt(1);
        checkEof();
    }

    @Test @Ignore
    public void testPosDecimal() // TODO rework?
        throws Exception
    {
        startTestCheckpoint("XXXtestPosDecimal");

        startIteration("+123d0");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkDecimal(123D);
        checkEof();
    }

    @Test
    public void testNegativeZeroDecimal()
        throws Exception
    {
        startTestCheckpoint("testNegativeZeroDecimal");

        startIteration("-0d0");
        nextValue();
        checkDecimal(-0.d); // TODO this should pass IonBigDecimal
        checkEof();
    }

    @Test @Ignore
    public void testPosFloat() // TODO rework?
        throws Exception
    {
        startTestCheckpoint("XXXtestPosFloat");

        startIteration("+123e0");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkFloat(123D);
        checkEof();
    }

    @Test @Ignore
    public void testPosTimestamp() // TODO rework?
        throws Exception
    {
        startTestCheckpoint("XXXtestPosTimestamp");

        startIteration("+2009-02-18");
        nextValue();
        checkSymbol("+");
        nextValue();
        checkTimestamp("2009-02-18");
        checkEof();
    }

    // JIRA ION-71
    @Test
    public void testTimestampWithRolloverOffset()
        throws Exception
    {
        startTestCheckpoint("testTimestampWithRolloverOffset");

        String text = "2009-10-01T00:00+01:00";
        startIteration(text);
        nextValue();
        checkTimestamp("2009-10-01T00:00+01:00");
        checkEof();
    }


    @Test
    public void testShortTimestamps()
        throws Exception
    {
        startTestCheckpoint("testShortTimestamps");

        String text = "2007T 2007-04T 2007-04-25 2007-04-25T";

        startIteration(text);
        nextValue();
        checkTimestamp("2007T");
        nextValue();
        checkTimestamp("2007-04T");
        nextValue();
        checkTimestamp("2007-04-25");
        nextValue();
        checkTimestamp("2007-04-25");
        checkEof();
    }

    @Test
    public void testTimestampWithZeroFraction()
        throws Exception
    {
        startTestCheckpoint("testTimestampWithZeroFraction");

        String text = "2009-11-23T17:04:03.0-04:00";
        startIteration(text);
        nextValue();
        checkTimestamp("2009-11-23T17:04:03.0-04:00");
        checkEof();
    }

    @Test
    public void testNullTimestamp()
        throws Exception
    {
        startTestCheckpoint("testNullTimestamp");

        String text = "null.timestamp";
        startIteration(text);
        nextValue();
        checkTimestamp((Timestamp)null);
        checkEof();
    }

    @Test
    public void testSpecialFloats()
        throws Exception
    {
        startTestCheckpoint("testSpecialFloats");

        startIteration("nan +inf -inf");
        nextValue();
        checkFloat(Double.NaN);
        nextValue();
        checkFloat(Double.POSITIVE_INFINITY);
        nextValue();
        checkFloat(Double.NEGATIVE_INFINITY);
        checkEof();
    }


    //=========================================================================

    @Test
    public void testSystemIterationShowsIvm()
        throws Exception
    {
        startTestCheckpoint("testSystemIterationShowsIvm");

        String text = ION_1_0;

        prepare(text);
        startSystemIteration();
        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        SymbolTable st = currentSymtab();

        // system readers don't necessarily support symbol tables
        // but if they do it better be the system table at this point
        if (st != null) {
            assertTrue("expected system table", st.isSystemTable());
            assertEquals(ION_1_0, st.getIonVersionId());
        }
        checkEof();
    }

    @Test
    public void testHighUnicodeDirectInBlob()
    {
        startTestCheckpoint("testHighUnicodeDirectInBlob");

        try {
            // JIRA ION-69
            loader().load("{{\ufffd}}");
            fail();
        } catch (final IonException e) {/* expected */}
    }

    // TODO test injected symtabs's symtab
    @Test @Ignore
    public void testSymtabOnInjectedSymtab()
        throws Exception
    {
        startTestCheckpoint("XXXtestSymtabOnInjectedSymtab");

        String text = "local";

        prepare(text);
        startSystemIteration();

        nextValue();
        checkSymbol(ION_1_0, ION_1_0_SID);
        SymbolTable st = currentSymtab();
        assertTrue(st.isSystemTable());
        assertEquals(ION_1_0, st.getIonVersionId());

        nextValue();
        checkType(IonType.STRUCT);
        assertSame(st, currentSymtab());

        nextValue();
        checkSymbol("local", systemMaxId() + 1);
        SymbolTable local = currentSymtab();
        assertTrue(local.isLocalTable());
        assertSame(st, local.getSystemSymbolTable());

        checkEof();
    }


    @Test // Trap for ION-173
    public void testDuplicateAnnotations()
    throws Exception
    {
        int sid = systemMaxId() + 1;

        startIteration("$ion_symbol_table::{symbols:[\"ann\"]} " +
        		"ann::ann::null");
        nextValue();
        checkAnnotations(new String[]{ "ann", "ann" },
                         new int[]{ sid, sid });
    }
}
