// Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.streaming;

import com.amazon.ion.DirectoryTestSuite;
import com.amazon.ion.FileTestCase;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonLoader;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.impl.IonBinaryWriterImpl;
import com.amazon.ion.impl.IonTextWriter;
import com.amazon.ion.impl.IonTreeWriter;
import com.amazon.ion.util.Equivalence;
import com.amazon.ion.util.Printer;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import junit.framework.TestSuite;

public class RoundTripStreamingTests extends DirectoryTestSuite
{
    static final boolean _debug_flag = false;

    public static TestSuite suite()
    {
        return new RoundTripStreamingTests();
    }

    public RoundTripStreamingTests()
    {
        super("good", "equivs");
    }

    @Override
    protected FileTestCase makeTest(File ionFile)
    {
        String fileName = ionFile.getName();
        // this test is here to get rid of the warning, and ... you never know
        if (fileName == null || fileName.length() < 1) throw new IllegalArgumentException("files should have names");
        return new StreamingRoundTripTest(ionFile);
    }


    @Override
    protected String[] getFilesToSkip()
    {
        return new String[]
        {
//             "equivs/stringU0001D11E.ion",
//             "equivs/stringU0120.ion",
//             "equivs/stringU2021.ion",
//             "equivs/symbols.ion",
//             "equivs/textNewlines.ion",
        };
    }


    private static class StreamingRoundTripTest
    extends FileTestCase
    {
        private Printer       myPrinter;
        private StringBuilder myBuilder;
        private byte[]        myBuffer;

        public StreamingRoundTripTest(File ionFile)
        {
            super(ionFile);

            FileInputStream in;
            BufferedInputStream bin;
            long len = ionFile.length();
            if (len < 0 || len > Integer.MAX_VALUE) throw new IllegalArgumentException("file too long for test");
            myBuffer = new byte[(int)len];
            try {
                in = new FileInputStream(myTestFile);
                bin = new BufferedInputStream(in);
                bin.read(myBuffer);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

        }

        @Override
        public void setUp()
            throws Exception
        {
            super.setUp();
            myPrinter = new Printer();
            myBuilder = new StringBuilder();
        }

        @Override
        public void tearDown()
            throws Exception
        {
            myPrinter = null;
            myBuilder = null;
            myBuffer  = null;
            super.tearDown();
        }

        private String makeString(IonDatagram datagram)
            throws IOException
        {
            boolean is_first = true;
            // iterator used to be .systemIterator()
            Iterator it = datagram.iterator();
            while (it.hasNext()) {
                IonValue value = (IonValue) it.next();
                if (is_first) {
                    is_first = false;
                }
                else {
                    myBuilder.append(' ');
                }
                myPrinter.print(value, myBuilder);
                // Why is this here?  myBuilder.append('\n');
            }

            String text = myBuilder.toString();
            myBuilder.setLength(0);
            return text;
        }

        private byte[] makeText(byte[] buffer, boolean prettyPrint)
        throws IOException
        {
            IonReader in = makeIterator(buffer);
            IonTextWriter tw = new IonTextWriter(prettyPrint);

            tw.writeValues(in);
            byte[] buf = tw.getBytes(); // this is utf-8

            return buf;
        }

        private String makeString(byte[] buffer)
        throws IOException
        {
            byte[] buf = makeText(buffer, false);
            String text = new String(buf, "UTF-8");

            return text;
        }


        private byte[] makeBinary(byte[] buffer)
        throws IOException
        {
            IonReader in = makeIterator(buffer);
            IonBinaryWriterImpl bw = system().newBinaryWriter();

            bw.writeValues(in);
            byte[] buf = bw.getBytes(); // this is binary

            return buf;
        }

        private byte[] makeBinary(IonDatagram datagram)
        {
            return datagram.getBytes();
        }

        private IonDatagram makeTree(byte[] buffer)
        throws IOException
        {
            IonReader in = makeIterator(buffer);
            IonTreeWriter tw = new IonTreeWriter(mySystem);

            tw.writeValues(in);
            IonValue v = tw.getContentAsIonValue();

            return (IonDatagram)v;
        }

        private static class roundTripBufferResults
        {
            roundTripBufferResults(String t) {
                title = t;
            }
            String      name;
            String      title;
            String      string;
            byte[]      utf8_buf;
            byte[]      utf8_pretty;
            byte[]      binary;
            IonDatagram ion;

            void compareResultsPass1(roundTripBufferResults other, IonLoader loader)
            {
                String vs = this.title + " vs " + other.title;
                //assertEquals("string: " + vs, this.string, other.string);
                compareStringAsTree("string: " + vs, other, loader);
                compareBuffers("utf8: " + vs, this.utf8_buf, other.utf8_buf);
                // We don't compare raw utf8_pretty buffers, since we shouldn't
                // care whether pretty-printing is byte-for-byte identical.
                compareUTF8AsTree("utf8: " + vs, other, loader);
                comparePrettyUTF8AsTree("pretty utf8: " + vs, other, loader);
                compareBuffers("binary: " + vs, this.binary, other.binary);
                compareBinaryAsTree("binary: " + vs, other, loader);

                boolean datagrams_are_equal = Equivalence.ionEquals(this.ion, other.ion);
                if (!datagrams_are_equal) {
                    // Note: we're dumping this.binary, but that isn't what's
                    // actually in the datagram.  So it could be misleading.
                    dump_datagrams(other);
                    datagrams_are_equal = Equivalence.ionEquals(this.ion, other.ion);
                }
                assertTrue("datagram: " + vs, Equivalence.ionEquals(this.ion, other.ion));
            }

            void compareResults(roundTripBufferResults other, IonLoader loader)
            {
                String vs = this.title + " vs " + other.title;
                assertEquals("string: " + vs, this.string, other.string);
                compareBuffers("utf8: " + vs, this.utf8_buf, other.utf8_buf);
                // We don't compare raw utf8_pretty buffers, since we shouldn't
                // care whether pretty-printing is byte-for-byte identical.
                compareBuffers("binary: " + vs, this.binary, other.binary);
                compareBinaryAsTree("binary: " + vs, other, loader);
                assertTrue("datagram: " + vs, Equivalence.ionEquals(this.ion, other.ion));
            }

            static void compareBuffers(String title, byte[] buf1, byte[] buf2) {
                assertTrue(title+" buf1 should not be null", buf1 != null);
                assertTrue(title+" buf2 should not be null", buf2 != null);

                // TODO later, when the namespace construction is fixed
                //             in both the iterators and the tree impl
                // assertEquals(title + " buffer lengths", buf1.length, buf2.length);
                // for (int ii=0; ii<buf1.length; ii++) {
                //  boolean bytes_are_equal = (buf1[ii] == buf2[ii]);
                //  assertTrue(title + " byte at "+ii, bytes_are_equal);
                // }
            }

            void compareStringAsTree(String title, roundTripBufferResults other, IonLoader loader) {
                // general approach:
                //      open tree over both, compare trees - use equiv
                //      open iterators over both, compare iterator contents
                IonDatagram dg1 = loader.load(this.string);
                IonDatagram dg2 = loader.load(other.string);

                if (dg1 == null || dg2 == null) {
                    assertTrue("if one datagram is null both must be null in "+title, dg1 == dg2);
                }
                boolean datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                if (!datagrams_are_equal) {
                    datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                }
                assertTrue("resulting datagrams should be the same in "+title, datagrams_are_equal);
            }

            void compareUTF8AsTree(String title, roundTripBufferResults other, IonLoader loader) {
                // general approach:
                //      open tree over both, compare trees - use equiv
                //      open iterators over both, compare iterator contents
                IonDatagram dg1 = loader.load(this.utf8_buf);
                IonDatagram dg2 = loader.load(other.utf8_buf);

                if (dg1 == null || dg2 == null) {
                    assertTrue("if one datagram is null both must be null in "+title, dg1 == dg2);
                }
                boolean datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                if (!datagrams_are_equal) {
                    datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                }
                assertTrue("resulting datagrams should be the same in "+title, datagrams_are_equal);
            }

            void comparePrettyUTF8AsTree(String title, roundTripBufferResults other, IonLoader loader) {
                // general approach:
                //      open tree over both, compare trees - use equiv
                //      open iterators over both, compare iterator contents
                IonDatagram dg1 = loader.load(this.utf8_pretty);
                IonDatagram dg2 = loader.load(other.utf8_pretty);

                if (dg1 == null || dg2 == null) {
                    assertTrue("if one datagram is null both must be null in "+title, dg1 == dg2);
                }
                boolean datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                if (!datagrams_are_equal) {
                    datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                }
                assertTrue("resulting datagrams should be the same in "+title, datagrams_are_equal);
            }

            void compareBinaryAsTree(String title, roundTripBufferResults other, IonLoader loader) {
                // general approach:
                //      open tree over both, compare trees - use equiv
                //      open iterators over both, compare iterator contents
                IonDatagram dg1 = loader.load(this.binary);
                IonDatagram dg2 = loader.load(other.binary);

                if (dg1 == null || dg2 == null) {
                    assertTrue("if one datagram is null both must be null in "+title, dg1 == dg2);
                }
                boolean datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                if (!datagrams_are_equal) {
                    dump_datagrams(other);
                    datagrams_are_equal = Equivalence.ionEquals(dg1, dg2);
                }
                assertTrue("resulting datagrams should be the same in "+title, datagrams_are_equal);
            }

            void dump_datagrams(roundTripBufferResults other) {
                System.out.println("\n------------------------------\n");
                System.out.println(this.string);
                dump_binary(this.title, this.binary);
                System.out.println("\n");
                dump_binary(other.title, other.binary);
            }

            void dump_binary(String title, byte[] buf)
            {
                System.out.println("dump buffer for "+title);
                if (buf == null) {
                    System.out.println(" <null>");
                }
                else {
                    int len = buf.length;
                    System.out.println(" length: "+len);
                    for (int ii=0; ii<len; ii++) {
                        int b = (buf[ii]) & 0xff;
                        if ((ii & 0xf) == 0) {
                            System.out.println();
                            String x = "     "+ii;
                            if (x.length() > 5)  x = x.substring(x.length() - 6);
                            System.out.print(x+": ");
                        }
                        String y = "00"+Integer.toHexString(b);
                        y = y.substring(y.length() - 2);
                        System.out.print(y+" ");
                    }
                    System.out.println();

                    for (int ii=0; ii<len; ii++) {
                        int b = (buf[ii]) & 0xff;
                        if ((ii & 0xf) == 0) {
                            System.out.println();
                            String x = "     "+ii;
                            if (x.length() > 5)  x = x.substring(x.length() - 6);
                            System.out.print(x+": ");
                        }
                        String y = "  " + (char)((b >= 32 && b < 128) ? b : ' ');
                        y = y.substring(y.length() - 2);
                        System.out.print(y+" ");
                    }
                    System.out.println();


                }

            }

        }

        roundTripBufferResults roundTripBuffer(String pass, byte[] testBuffer)
        throws IOException
        {
            roundTripBufferResults stream =
                new roundTripBufferResults(pass + " stream");
            roundTripBufferResults tree =
                new roundTripBufferResults(pass + " tree");

            stream.name = this.getName() + " (as stream)";
            tree.name = this.getName() + " (as IonValue)";

            // load() takes ownership of the buffer
            IonDatagram inputDatagram = loader().load(testBuffer.clone());

            // Turn the DOM back into text...
            tree.string     = makeString(inputDatagram);
            tree.utf8_buf   = tree.string.getBytes("UTF-8");
            tree.utf8_pretty = tree.string.getBytes("UTF-8"); // FIXME hack
            tree.binary     = makeBinary(inputDatagram);
            tree.ion        = inputDatagram;
            checkBinaryHeader(tree.binary);

            stream.utf8_buf = makeText(testBuffer, false);
            stream.utf8_pretty = makeText(testBuffer, true);
            stream.string   = makeString(testBuffer);
            stream.binary   = makeBinary(testBuffer);
            stream.ion      = makeTree(testBuffer);
            checkBinaryHeader(stream.binary);

            tree.compareResultsPass1(stream, myLoader);

            return stream;
        }

        IonReader makeIterator(byte [] testBuffer) {
            IonReader inputIterator = system().newReader(testBuffer);
            return inputIterator;
        }


        @Override
        public void runTest()
            throws Exception
        {
            // general round trip plan for the streaming interfaces:
            //
            //  open iterator over test file
            //  create datagram over test file
            //  get string from datagram
            //  get binary from datagram
            //  pass iterator to text writer
            //     compare output with string from datagram
            //  pass iterator to binary writer
            //     compare output with binary from datagram
            //  pass iterator to tree writer
            //     output with datagram
            //  test comparison again with the resulting binary
            //  and resulting text (1 level recurse or 2?

            if (this.getName().startsWith("testfile35")) {
                // FIXME - we need to fix the problem with in line text symbol tables !!
                if (_debug_flag) {
                    System.out.println("debugging testfile35, with in line text symbol tables");
                }
                else {
                    System.err.println(getName() + ": skipping testfile35");
                    return;
                }
            }

            roundTripBufferResults pass1 = roundTripBuffer("P1: buffer", myBuffer);
            roundTripBufferResults pass2bin = roundTripBuffer("P2: binary",pass1.binary);
            roundTripBufferResults pass2text = roundTripBuffer("P3: utf8", pass1.utf8_buf);
            roundTripBufferResults pass2pretty = roundTripBuffer("P3: utf8", pass1.utf8_pretty);
            if (pass2bin == pass2text && pass2pretty == pass2pretty) {
                // mostly to force these to be used (pass2*)
                throw new RuntimeException("boy this is odd");
            }
        }
    }



}
