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

package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.impl.IonBinary.BufferManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import org.junit.After;
import org.junit.Test;


public class ByteBufferTest
    extends IonTestCase
{
    @Override @After
    public void tearDown()
        throws Exception
    {
        super.tearDown();
        BlockedBuffer.resetParameters();
    }

    @Test
    public void testSmallInsertion()
    {
        BufferManager buf = new BufferManager();
        IonBinary.Writer writer = buf.openWriter();

        // Write enough data to overflow the first block
        byte[] initialData = new byte[BlockedBuffer._defaultBlockSizeMin + 5];

        // Now insert some stuff at the beginning
        try {
            writer.write(initialData, 0, initialData.length);
            writer.setPosition(0);
            writer.insert(10);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    @Test
    public void testUnicodeCodepointOverflow() {
        BufferManager buf = new BufferManager ();
        IonBinary.Writer writer = buf.openWriter();

        try {
            writer.writeStringData(new String(new char[] { 0xd799 }, 0, 1));
            writer.writeStringData(new String(new char[] { 0xe000 }, 0, 1));
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
        try {
            writer.writeStringData(new String(new char[] { 0xd800 }, 0, 1));
            fail("Successfully parsed a partial surrogate");
        } catch (Exception e) {
        }
        try {
            writer.writeStringData(new String(new char[] { 0xdfff}, 0, 1));
            fail("Successfully parsed a partial surrogate");
        } catch (Exception e) {
        }
    }

    @Test
    public void testUnicodeCodepointOverflowStatic() {
        OutputStream os = new ByteArrayOutputStream();
        try {
            IonBinary.writeString(os, new String(new char[] { 0xd799 }, 0, 1));
            IonBinary.writeString(os, new String(new char[] { 0xe000 }, 0, 1));
            // surrogates
            IonBinary.writeString(os, new String(new char[] { 0xd800, 0xdc00 }, 0, 2));
            IonBinary.writeString(os, new String(new char[] { 0xdbff, 0xdfff }, 0, 2));
        } catch (Exception e) {
            throw new IonException(e);
        }
        try {
            IonBinary.writeString(os, new String(new int[] { 0xd800 }, 0, 1));
            fail("Successfully parsed a partial surrogate");
        } catch (Exception e) {
        }
        try {
            IonBinary.writeString(os, new String(new int[] { 0xdfff}, 0, 1));
            fail("Successfully parsed a partial surrogate");
        } catch (Exception e) {
        }
    }

    @Test
    public void testLargeInsertion()
    {
        BufferManager buf = new BufferManager ();
        IonBinary.Writer writer = buf.openWriter();

        // Write enough data to overflow the first block
        byte[] initialData = new byte[BlockedBuffer._defaultBlockSizeMin + 5];

        // Now insert lots of stuff at the beginning
        try {
            writer.write(initialData, 0, initialData.length);
            writer.setPosition(0);
            writer.insert(5000);
        }
        catch (IOException e) {
            throw new IonException(e);
        }
    }

    @Test
    public void testRandomUpdatesSmall() throws Exception
    {
        testRandomUpdates(7, 19, 100);
    }

    @Test
    public void testRandomUpdatesMedium() throws Exception
    {
        testRandomUpdates(128, 4096, 1000);
    }

    @Test
    public void testRandomUpdatesLarge()  throws Exception
    {
        testRandomUpdates(32*1024, 32*1024, 1000);
    }

    final static boolean _debug_long_test = false;

    @Test
    public void testRandomUpdatesLargeAndLong()  throws Exception
    {
        // TODO: turn on a long test, maybe not this long, when we can know it's
        //       not going to be oppressive
        if (!_debug_long_test) return;
        testRandomUpdates(7, 19, 100000);
    }

    @Test
    public void testPreviousFailuresInRandomUpdates() throws Exception
    {
        testRandomUpdates(128, 4096, 1000, 1230074384739L);
    }

    private void testRandomUpdates(int min, int max, int count) throws Exception
    {
    	long seed = System.currentTimeMillis();
    	testRandomUpdates(min, max, count, seed);
    }

    private void testRandomUpdates(int min, int max, int count, long seed) throws Exception
    {
        BlockedBuffer.setBlockSizeParameters(min, max); // make it work hard

        testByteBuffer testBuf = new testByteBuffer();

        BlockedBuffer blocked = new BlockedBuffer();
        BlockedBuffer.BlockedByteOutputStream blkout = new BlockedBuffer.BlockedByteOutputStream(blocked);
        BlockedBuffer.BlockedByteInputStream  blkin = new BlockedBuffer.BlockedByteInputStream(blocked);


        final boolean debug_output = false;
        final boolean checkedOften = false;
        final boolean checkContentsEverytime = false;

        final int max_update = 1000;
        final int rep_count  = count;

        final int choiceCheck  = 0;
        final int choiceAppend = 1;
        final int choiceInsert = 2;
        final int choiceRemove = 3;
        final int choiceWrite  = 4;

        Random r = new Random(seed);
        if (debug_output) System.out.println("seed: " + seed);

        byte[]  data = new byte[max_update];
        int     val = -1, len = -1, pos = 0;
        boolean justChecked = false;

        try {
            for (int ii=0; ii<rep_count; ii++) {
                int choice = r.nextInt(5);
                if (checkedOften && !justChecked) choice = choiceCheck;

                switch(choice) {
                    case choiceCheck: // check
                        // check doesn't need anything else
                        if (justChecked) continue;
                        justChecked = true;
                        break;

                    case choiceInsert: // insert
                    case choiceRemove: // remove
                    case choiceWrite:  // write
                        justChecked = false;
                        if (testBuf.limit() < 1) continue;
                        pos = r.nextInt(testBuf.limit());
                        break;
                    case choiceAppend: // append
                        justChecked = false;
                        pos = testBuf.limit();
                        break;
                    default:
                        assert "" == "this is a bad case in a switch statement";
                        throw new RuntimeException("switch case error!");
                }


                switch(choice) {
                    case choiceCheck: // check
                        break;

                    case choiceInsert: // insert
                    case choiceRemove: // remove
                    case choiceWrite:  // write
                    case choiceAppend: // append
                        len = r.nextInt(max_update);
                        if (choice != choiceRemove) {
                            // every choice except remove needs a prep'd buffer
                            val = r.nextInt(32);
                            val |= (choice << 5);
                            for (int jj=0; jj<len; jj++) {
                                data[jj] = (byte)(0xff & val);
                            }
                        }
                        // position the buffers for the operation
                        testBuf.position(pos);
                        // old: buf.positionForWrite(pos);
                        blkout.setPosition(pos);
                        break;
                }

                switch(choice) {
                    case choiceCheck: // check
                        if (debug_output) System.out.println("check "+testBuf.limit());
                        // old: assert testBuf.limit() == buf.size();
                        if (!checkContentsEverytime && r.nextInt(1000) < 990) break; // don't do this too often
                        if (testBuf.limit() > 0) {
                            testBuf.position(0);
                            // old: buf.positionForRead(0);
                            blkin.sync();
                            blkin.setPosition(0);
                            for (int jj = 0; jj<testBuf.limit(); jj++) {
                                int bt = testBuf.read();
                                // buf: int br = buf.read();
                                int bk = blkin.read();
                                if (jj > 32688 && jj < 32690) {
                                    // old: assert (byte)(bt & 0xff) == (byte)(br & 0xff);
                                    assert (byte)(bt & 0xff) == (byte)(bk & 0xff);
                                }
                                else {
                                    // old: assert (byte)(bt & 0xff) == (byte)(br & 0xff);
                                    assert (byte)(bt & 0xff) == (byte)(bk & 0xff);
                                }
                            }
                            // old: buf._validate();
                            blkin._validate();
                        }
                        break;
                    case choiceAppend: // append
                        if (debug_output) System.out.println("append "+len+" of "+val+" at "+pos);
                        testBuf.write(data, len);
                        // old: buf.write(data, 0, len);
                        blkout.write(data, 0, len);
                        break;
                    case choiceInsert: // insert
                        if (debug_output) System.out.println("insert "+len+" of "+val+" at "+pos);
                        testBuf.insert(data, len);
                        // old: buf.insert(len);
                        // old: buf.write(data, 0, len);
                        blkout.insert(data, 0, len);
                        break;
                    case choiceRemove: // remove
                        if (pos + len > testBuf.limit()) {
                            if (r.nextInt(100) < 90) break;
                            len = testBuf.limit() - pos;
                        }
                        if (debug_output) System.out.println("remove "+len+" at "+pos);
                        testBuf.remove(len);
                        // old: buf.remove(len);
                        blkout.remove(len);
                        break;
                    case choiceWrite: // write
                        if (pos + len > testBuf.limit()) {
                            if (r.nextInt(100) < 90) break;
                            len = testBuf.limit() - pos;
                        }
                        if (debug_output) System.out.println("write "+len+" of "+val+" at "+pos);
                        testBuf.write(data, len);
                        // old: buf.write(data, 0, len);
                        blkout.write(data, 0, len);
                        break;
                    default:
                        assert "" == "this is a bad case in a switch statement";
                    throw new RuntimeException("switch case error!");
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("FAILED with seed: " + seed, e);
        }
        catch (Error e) {
            throw new RuntimeException("FAILED with seed: " + seed, e);
        }
    }

    static public class testByteBuffer
    {
        static final int startingBufferSize = 16;
        int    _position;
        int    _inUse;
        byte[] _buf;

        public testByteBuffer() {
            this._buf = new byte[startingBufferSize];
        }

        void expand(int newlen) {
            int len = this._buf.length;
            while (len < newlen) {
                len *= 2;
            }
            if (len > this._buf.length) {
                byte[] newbuf = new byte[len];
                System.arraycopy(this._buf, 0, newbuf, 0, this._inUse);
                this._buf = newbuf;
            }
        }
        public int limit() { return this._inUse; }
        public int position(int pos) {
            assert (pos >= 0 && pos <= _inUse);
            this._position = pos;
            return this._position;
        }
        public int insert(byte[] data, int len) {
            position(this._position);
            int newEnd = this._inUse + len;
            expand(newEnd);
            System.arraycopy(this._buf, this._position
                            ,this._buf, this._position + len, this._inUse - this._position);
            System.arraycopy(data, 0, this._buf, this._position, len);
            this._position += len;
            this._inUse += len;
            return len;
        }
        public int remove(int len) {
            position(this._position);
            assert (this._position + len <= this._inUse);
            System.arraycopy(this._buf, this._position + len
                            ,this._buf, this._position, this._inUse - (this._position + len));
            this._inUse -= len;
            return len;
        }
        public int read() {
            position(this._position);
            assert(this._position + 1 <= this._inUse);
            int ret = this._buf[this._position];
            this._position++;
            return ret;
        }
        public byte[] read(int len) {
            position(this._position);
            assert(this._position + len <= this._inUse);
            byte[] ret = new byte[len];
            System.arraycopy(this._buf, this._position, ret, 0, len);
            this._position += len;
            return ret;
        }
        public int write(byte[] data, int len) {
            assert(data.length >= len);
            position(this._position);
            int endWrite = this._position + len;
            expand(endWrite);
            System.arraycopy(data, 0, this._buf, this._position, len);
            this._position = endWrite;
            if (this._position > this._inUse) this._inUse = this._position;
            return len;
        }
    }
}
