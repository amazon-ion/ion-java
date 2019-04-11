
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
/**
 * This implements a blocked byte buffer and both an input and output stream
 * that operates over it. It is designed to be able to be randomly accessed.
 * The output steam supports both inserting data (with "stretching") in the
 * middle of the stream and over-write.  The output steam also supports remove
 * which shrinks the overall data buffer. The underlying buffer is backed by
 * one or more byte arrays to minimize data movement.
 * <p>
 * It is also meant to be reused, so that it does not have to pressure the
 * GC, if that is desirable.
 */
final class BlockedBuffer
{
    ///////////////////////////////////////////////////////////////////////////////
    //
    // updatable, insertable, and possibly fragmented byte buffer
    //
    // these manage the set of memory (byte) buffers
    ArrayList<bbBlock>  _blocks;
    int                 _next_block_position;   // next position in _blocks for active block, may be less than _blocks.size()
    int                 _lastCapacity;          // used to allocate new blocks
    int                 _buf_limit;             // high water mark of _position
    int                 _version;
    int                 _mutation_version;
    Object              _mutator;
// BUGBUG - this is just a test, it shouldn't be in checked in code
static final boolean test_with_no_version_checking = false;
    void start_mutate(Object caller, int version) {
        if (test_with_no_version_checking) return;
        if (_mutation_version != 0 || _mutator != null)
            throw new BlockedBufferException("lock conflict");
        if (version != _version)
            throw new BlockedBufferException("version conflict on update");
        _mutator = caller;
        _mutation_version = version;
    }
    int end_mutate(Object caller) {
        if (test_with_no_version_checking) return _version;
        if (_version != _mutation_version)
            throw new BlockedBufferException("version mismatch failure");
        if (caller != _mutator)
            throw new BlockedBufferException("caller mismatch failure");
        _version = _mutation_version + 1;
        _mutation_version = 0;
        _mutator = null;
        return _version;
    }
    boolean mutation_in_progress(Object caller, int version) {
        if (test_with_no_version_checking) return false;
        if (_mutation_version != version)
            throw new BlockedBufferException("unexpected update lock conflict");
        if (caller != _mutator)
            throw new BlockedBufferException("caller mismatch failure");
        return true;
    }
    int getVersion() {
        return _version;
    }
    static boolean debugValidation = false;
    static int _defaultBlockSizeMin;
    static int _defaultBlockSizeUpperLimit;
    static {
        resetParameters();
    }
    public static void resetParameters() {
        debugValidation = false;
        _defaultBlockSizeMin = 4096 * 8;
        _defaultBlockSizeUpperLimit = 4096 * 8;
    }
    public int _blockSizeMin = _defaultBlockSizeMin;
    public int _blockSizeUpperLimit = _defaultBlockSizeUpperLimit;
    static void setBlockSizeParameters(int min, int max,
                                              boolean intenseValidation) {
        debugValidation = intenseValidation;
        setBlockSizeParameters(min, max);
    }
    public static void setBlockSizeParameters(int min, int max) {
        if (min < 0 || max < min) {
            throw new IllegalArgumentException();
        }
        _defaultBlockSizeMin           = min;
        _defaultBlockSizeUpperLimit    = max;
        return;
    }
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Creates a new buffer without preallocating any space.
     */
    public BlockedBuffer() {
        start_mutate(this, 0);
        init(0, null);
        end_mutate(this);
    }
    /**
     * Creates a new buffer, preallocating some initial capacity.
     *
     * @param initialSize the number of bytes to allocate.
     */
    public BlockedBuffer(int initialSize) {
        start_mutate(this, 0);
        init(initialSize, null);
        end_mutate(this);
    }
    /**
     * Creates a new buffer, assuming ownership of given data.
     * <em>This method assumes ownership of the <code>data</code> array</em>
     * and will modify it at will.
     *
     * @param data the initial data to be buffered.
     *
     * @throws NullPointerException if buffer is null.
     */
    public BlockedBuffer(byte[] data) {
        start_mutate(this, 0);
        init(0, new bbBlock(data));
        _buf_limit = data.length;
        end_mutate(this);
    }
    /**
     * Creates a new buffer containing all data remaining on an
     * {@link InputStream}.  The stream is closed before returning.
     *
     * @param data must not be null.
     *
     * @throws IOException
     */
    public BlockedBuffer(InputStream data)
        throws IOException
    {
        IonBinary.Writer writer = new IonBinary.Writer(this);
        try {
            writer.write(data);
        }
        finally {
            data.close();
        }
    }
    /**
     * creates a logical copy of the buffer.  This does not preserve
     * the position state and is equivalent to constructing a new
     * buffer from the old by getting the bytes from the original
     * and writing them to a new buffer.
     */
    @Override
    public BlockedBuffer clone()
    {
        BlockedBuffer clone = new BlockedBuffer(this._buf_limit);
        int end = this._buf_limit;
        bbBlock dst_block = clone._blocks.get(0);
        int dst_offset = 0;
        int dst_limit = dst_block.blockCapacity();
        for (int ii=0; ii<this._blocks.size(); ii++) {
            bbBlock src_block = this._blocks.get(ii);
            if (src_block._limit < 1) continue; // see if there's any interesting data in this block
            int src_end = src_block._limit + src_block._offset;
            int to_copy = src_block._limit;
            if (to_copy > dst_limit - dst_offset) {
                to_copy = dst_limit - dst_offset;
            }
            System.arraycopy(src_block._buffer, 0, dst_block._buffer, dst_offset, to_copy);
            dst_offset += to_copy;
            // the cloned BlockedBuffer should be able to hold all the data
            // in it's single block
            assert dst_offset <= dst_limit;
            // see if we're done (and break out in that case)
            if (src_end >= end) break;
        }
        dst_block._limit = dst_offset;
        clone._buf_limit = dst_offset;
        return clone;
    }
    /**
     * Initializes the various members such as the block arraylist
     * the initial block and the various values like the block size upper limit.
     * @param initialSize or 0
     * @param initialBlock or null
     * @return bbBlock the initial current block
     */
    private bbBlock init(int initialSize, bbBlock initialBlock)
    {
        this._lastCapacity = BlockedBuffer._defaultBlockSizeMin;
        this._blockSizeUpperLimit = BlockedBuffer._defaultBlockSizeUpperLimit;
        while (this._lastCapacity < initialSize &&
               this._lastCapacity < this._blockSizeUpperLimit)
        {
            this.nextBlockSize(this, 0);
        }
        int count = initialSize / this._lastCapacity;
        if (initialBlock != null) count = 1;
        this._blocks = new ArrayList<bbBlock>(count);
        if (initialBlock == null) {
            initialBlock = new bbBlock(this.nextBlockSize(this, 0));
        }
        this._blocks.add(initialBlock);
        this._next_block_position = 1;
        // create any preallocated blocks (following _next_block_position)
        bbBlock b;
        for (int need = initialSize - initialBlock.blockCapacity()
           ; need > 0
           ; need -= b.blockCapacity()
        ) {
            b = new bbBlock(this.nextBlockSize(this, 0));
            b._idx = -1;
            this._blocks.add(b);
        }
        return initialBlock;
    }
    /**
     * Gets the number of bytes of content in this buffer.
     * This isn't the same as its capacity.
     */
    public final int size() {
        return _buf_limit;
    }
    /**
     * empties the entire contents of the buffer
     */
    private void clear(Object caller, int version) {
        assert mutation_in_progress(caller, version);
        _buf_limit = 0;
        for (int ii=0; ii<_blocks.size(); ii++) {
            _blocks.get(ii).clearBlock();
            // _blocks.get(ii)._idx = -1; this is done in clearBlock()
        }
        bbBlock first = _blocks.get(0);
        first._idx = 0;                        // cas: 26 dec 2008
        first._offset = 0;
        first._limit = 0;
        _next_block_position = 1;
        return;
    }
    /**
     * treat the limit as the end of file
     */
    bbBlock truncate(Object caller, int version, int pos) {
        assert mutation_in_progress(caller, version);
        if (0 > pos || pos > this._buf_limit )
            throw new IllegalArgumentException();
        // clear out all the blocks in use from the last in use
        // to the block where the eof will be located
        bbBlock b = null;
        for (int idx = this._next_block_position - 1; idx >= 0; idx--) {
            b = this._blocks.get(idx);
            if (b._offset <= pos) break;
            b.clearBlock();
        }
        if (b == null) {
            throw new IllegalStateException("block missing at position "+pos);
        }
        // reset the next block position to account for this.
        this._next_block_position = b._idx + 1;
        // on the block where eof is, set it's limit appropriately
        b._limit = pos - b._offset;
        // set the overall buffer limits
        this._buf_limit = pos;
        b = this.findBlockForRead(pos, version, b, pos);
        return b;
    }
    private bbBlock addBlock(Object caller, int version, int idx, int offset,
                             int needed)
    {
        assert mutation_in_progress(caller, version);
        bbBlock newblock = null;
        for (int ii=this._next_block_position; ii < this._blocks.size(); ii++)
        {
            bbBlock tmpblock = this._blocks.get(this._next_block_position);
            if (tmpblock._buffer.length >= needed) {
                this._blocks.remove(this._next_block_position);
                newblock = tmpblock;
                break;
            }
        }
        if (newblock == null) {
            // if there's nothing big enough to recycle
            // so we have to really make more space
            int bufcapacity = 0;
            if (needed > _blockSizeUpperLimit) {
                bufcapacity = needed;
            }
            else {
                while (bufcapacity < needed) {
                    bufcapacity = this.nextBlockSize(caller, version);
                }
            }
            newblock = new bbBlock(bufcapacity);
        }
        // if the caller didn't specify an index
        // we'll have to find out where this goes
        if (idx == -1) {
            for (idx = 0; idx < this._next_block_position; idx++) {
                if (this._blocks.get(idx)._offset < 0) {
                    break;
                }
                if (offset >= this._blocks.get(idx)._offset) {
                    break;
                }
            }
        }
        // initialize the buffer and add it to the list in the right spot
        newblock._idx = idx;
        newblock._offset = offset;
        _blocks.add(idx, newblock);
        _next_block_position++;
        // if this isn't the last buffer, bump the idx of the trailing buffers
        for (int ii = idx + 1; ii < _next_block_position; ii++) {
            this._blocks.get(ii)._idx = ii;
        }
        return newblock;
    }
    private int nextBlockSize(Object caller, int version)
    {
        assert mutation_in_progress(caller, version);
        if (_lastCapacity == 0) {
            _lastCapacity = _blockSizeMin;
        }
        else if (_lastCapacity < _blockSizeUpperLimit) {
            _lastCapacity *= 2;
        }
        return _lastCapacity;
    }
    // starts with (pos, 0, _next_block_position) so we're really
    // looking in blocks with indices from lo to (hi-1) inclusive
    final bbBlock findBlockHelper(int pos, int lo, int hi)
    {
        bbBlock block;
        int     ii;
        if ((hi - lo) <= 3) {
            for (ii=lo; ii<hi; ii++) {
                block = this._blocks.get(ii);
                if (pos > block._offset + block._limit) continue;
                if (block.containsForRead(pos)) {
                    return block;
                }
                if (block._offset >= pos) break;
            }
            return this._blocks.get(ii - 1);    // this will always be > 0
        }
        int mid = (hi + lo) / 2;
        block = this._blocks.get(mid);
        assert block != null;
        if (block._offset > pos) {
            return findBlockHelper(pos, lo, mid);
        }
        return findBlockHelper(pos, mid, hi);
    }
    /**
     * find the block where this offset (newPosition) has already
     * been written. Typically the caller will set _curr to be the
     * returned block.
     * @param pos global position to be read from
     * @return curr block ready to be read from
     */
    bbBlock findBlockForRead(Object caller, int version, bbBlock curr, int pos)
    {
        assert pos >= 0 && "buffer positions are never negative".length() > 0;
        if (pos > this._buf_limit) {
            throw new BlockedBufferException("invalid position");
        }
        assert _validate();
        if (curr != null) {
            if (curr.containsForRead(pos)) {
                return curr;
            }
            if (pos == this._buf_limit && (pos - curr._offset) == curr._limit) {
                return curr;
            }
        }
        boolean at_eof = (pos == this._buf_limit);
        if (at_eof) {
            // if this is the last block actually in use
            // and we're looking for the eof position then
            // we can check for the "last byte not quite
            // written yet" case, which is fine
            bbBlock block = this._blocks.get(this._next_block_position - 1);
            if (block.containsForWrite(pos)) return block;
        }
        else {
            bbBlock block = this.findBlockHelper(pos, 0, this._next_block_position);
            return block;
        }
        throw new BlockedBufferException("valid position can't be found!");
    }
        /**
     * find the block where this offset (newPosition) should be written.
     * typicall the caller will set _curr to be the returned block.
     * @param pos global position to be written to
     * @return curr block ready to be written to
     */
    bbBlock findBlockForWrite(Object caller, int version, bbBlock curr, int pos)
    {
        assert mutation_in_progress(caller, version);
        assert (pos >= 0 && "invalid position, positions must be >= 0".length() > 0);
        if (pos > this._buf_limit + 1) {
            throw new BlockedBufferException("writes must be contiguous");
        }
        assert _validate();
        if (curr != null && curr.hasRoomToWrite(pos, 1) == true) {
            if (curr._offset + curr._limit == pos && curr._idx < this._next_block_position) {
                bbBlock b = this._blocks.get(curr._idx + 1);
                if (b.containsForWrite(pos)) {
                    curr = b;
                }
            }
            return curr;
        }
        // we're not going to write into curr, so find out the right block
        bbBlock block;
        if (pos == this._buf_limit) {
            // if we're at the limit the only possible (existing) block
            // will be the very last block - shortcut to optimize append
            assert this._next_block_position > 0;
            block = this._blocks.get(this._next_block_position - 1);
        }
        else if (curr != null && pos == curr._offset + curr._limit) {
            // if our current position is exactly at the end (and we already know
            // we can't write into this block if we can write at all we'll have
            // to write into the next block (inner blocks can't be 0 bytes long)
            block = this._blocks.get(curr._idx + 1);
        }
        else {
            // since we're not at the limit and we don't have a current block
            // we'll go find the block in the list (this is an abnormal case)
            // since append if usual for writing
            block = findBlockHelper(pos, 0, this._next_block_position);
        }
        assert block != null;
        assert block.containsForWrite(pos);
        // chech our candidate block to see if it's the one we'd write into
        if (block.hasRoomToWrite(pos, 1)) {
            return block;
        }
        // at this point, we can't use _curr in any event so we can just
        // move on to the next block since findHelper will have returned
        // either the right block (which it didn't) or the one just in
        // front of the right block - so let's see if there is an allocated
        // block just following this
        if (block._idx < this._next_block_position - 1) {
            block = this._blocks.get(block._idx + 1);
            return block;
        }
        // there wasn't a following block (actually a common case when
        // you're appending) so we have to go ahead an actually add a new block
        int newIdx = block._idx + 1;
        assert newIdx == this._next_block_position;
        bbBlock ret =  this.addBlock(caller
                            ,version
                            ,newIdx
                            ,pos
                            ,this.nextBlockSize(caller, version)
               );
        return ret;
    }
    /**
     * dispatcher for the various forms of insert we encounter
     * calls one of the four helpers depending on the case
     * that is needed to inser here
     * @param len number of bytes to make space for
     * @return int number of bytes inserted
     */
    int insert(Object caller, int version, bbBlock curr, int pos, int len)
    {
        assert mutation_in_progress(caller, version);
        // DEBUG: int amountMoved = 0;
        // DEBUG: int before = this._buf_limit;
        // DEBUG: assert _validate();
        // if there's room in the current block - just
        // move the "trailing" bytes down and we're done
        int neededSpace = len - curr.unusedBlockCapacity();
        if (neededSpace <= 0) {
            // we have all the space we need in the current block
            // DEBUG: amountMoved =
            insertInCurrOnly(caller, version, curr, pos, len);
        }
        else {
            // we'll need at least some additional space beyond the curr
            // block, see if there's room in the
            // next one, otherwise we'll make more (blocks)
            bbBlock next = null;
            if (curr._idx < this._next_block_position - 1) {
                // if there is another block
                next = this._blocks.get(curr._idx + 1);
            }
            if (next != null &&
                (neededSpace <= next.unusedBlockCapacity())
            ) {
                // with the addition of the free space in the following block we have enough
                // DEBUG: amountMoved =
                insertInCurrAndNext(caller, version, curr, pos, len, next);
            }
            else {
                // we'll have to make one or more new blocks
                // first figure out much will be in the first
                // and last blocks (i.e. ignoring the whole
                // blocks
                int lenNeededInLastAddedBlock = neededSpace % _blockSizeUpperLimit;
                int tailLen = curr.bytesAvailableToRead(pos);
                if (lenNeededInLastAddedBlock < tailLen) lenNeededInLastAddedBlock = tailLen;
                if (lenNeededInLastAddedBlock < neededSpace
                 && neededSpace < this._blockSizeUpperLimit) {
                    // if we need less than the largest block then we should
                    // make *one* block with all of the needed space
                    lenNeededInLastAddedBlock = neededSpace;
                }
                bbBlock newblock = insertMakeNewTailBlock(caller, version, curr, lenNeededInLastAddedBlock);
                // now see if the curr block and this newblock have enough
                // available space to do the job, and if there's some trailing
                // data from curr that will end up staying in curr
                if (len <= (curr.unusedBlockCapacity()
                           + newblock.unusedBlockCapacity())
                ) {
                    // insert this as a zero length block immediately  after _curr
                    // insertBlock also adjusts the trailing blocks idx values
                    insertBlock(newblock);
                    // now pretend we just have the "push into the next block" case
                    // DEBUG: amountMoved =
                    insertInCurrAndNext(caller, version, curr, pos, len, newblock);
                }
                else {
                    // and last we have the case of having to insert more than 1 block
                    // which means all of the trailing bytes in _curr move into the last
                    // block
                    // DEBUG: amountMoved =
                    insertAsManyBlocksAsNeeded(caller, version, curr, pos, len, newblock);
                }
            }
        }
        // DEBUG: if (this._buf_limit - before != len
        // DEBUG: || amountMoved != len) {
        // DEBUG: throw new BlockedBufferException("insert went wrong #1 !!!");
        // DEBUG: }
        assert _validate();
        return len;
    }
    /**
     *  this handles insert when there's enough room in the
     *  current block
     */
    private int insertInCurrOnly(Object caller, int version, bbBlock curr, int pos, int len)
    {
        assert mutation_in_progress(caller, version);
        // the space we need is available right in the block
        assert curr.unusedBlockCapacity() >= len;
        System.arraycopy(curr._buffer, curr.blockOffsetFromAbsolute(pos)
                         ,curr._buffer, curr.blockOffsetFromAbsolute(pos) + len, curr.bytesAvailableToRead(pos));
        curr._limit += len;
        this.adjustOffsets(curr._idx, len, 0);
        notifyInsert(pos, len);
        return len;
    }
    private int insertInCurrAndNext(Object caller, int version, bbBlock curr, int pos, int len, bbBlock next)
    {
        assert mutation_in_progress(caller, version);
        // DEBUG: int amountMoved = 0;
        // all the space we need (len) fits in these two blocks
        assert curr.unusedBlockCapacity() + next.unusedBlockCapacity() >= len;
        // and we need to use space in both of these blocks
        assert curr.unusedBlockCapacity() < len;
        int availableToRead = curr.bytesAvailableToRead(pos);
        int tailInCurr = availableToRead;
        int deltaOfNextData = len - curr.unusedBlockCapacity();
        int tailCopiedToNext = deltaOfNextData;
        if (tailCopiedToNext > availableToRead) {
            tailCopiedToNext = availableToRead;
        }
        // first we copy the data in the next block down to make room
        // for data we're pushing off the end of the _curr block
        // if we need to, there may not be any data in the next block
        if (next._limit > 0) {
            System.arraycopy(next._buffer, 0, next._buffer, deltaOfNextData, next._limit);
        }
        next._limit += deltaOfNextData;
        // DEBUG: amountMoved += deltaOfNextData;
        // next we copy the data from the tail of _curr into the front of next
        // since we don't have room for it any longer in the _curr block
        // but it is possible that there is not tail at all (pos == limit)
        if (tailCopiedToNext > 0) {
            System.arraycopy(curr._buffer, curr._limit - tailCopiedToNext
                            , next._buffer, deltaOfNextData - tailCopiedToNext, tailCopiedToNext);
        }
        // finally if there's any tail left in the _curr block we copy that
        // down to the end of the _curr block (if all of the tail moved into
        // the next block nothing happens here
        int leftInCurr = tailInCurr - tailCopiedToNext;
        if (leftInCurr > 0) {
            int blockPosition = curr.blockOffsetFromAbsolute(pos);
            System.arraycopy(curr._buffer, blockPosition
                            ,curr._buffer, blockPosition + len, leftInCurr);
        }
        // finally if we reused from space in _curr (between _limit and the unreserved capacity)
        // we adjust for that as well as the space adjusted in the newblock
        int addedInCurr = curr.unusedBlockCapacity();
        if (addedInCurr > 0) {
            curr._limit += addedInCurr;
            // DEBUG: amountMoved += addedInCurr;
            next._offset += addedInCurr;
        }
        assert (curr.blockOffsetFromAbsolute(pos) + tailCopiedToNext + addedInCurr + leftInCurr) == curr._limit;
        this.adjustOffsets(next._idx, len, 0);
        notifyInsert(pos, len);
        // DEBUG: if (amountMoved != len) {
        // DEBUG: throw new BlockedBufferException("insert went wrong #4 !!!");
        // DEBUG: }
        return len;
    }
    private bbBlock insertMakeNewTailBlock(Object caller, int version, bbBlock curr, int minimumBlockSize)
    {
        assert mutation_in_progress(caller, version);
        // needed is the amount of data we'll put into the
        // final added block (which is actually added first)
        int newblocksize = minimumBlockSize;
        if (newblocksize < _blockSizeUpperLimit) {
            // if we don't need an oversize block then find a block
            // size that will be big enough
            while ((newblocksize = this.nextBlockSize(caller, version)) < minimumBlockSize) {
                // bump up requested block capacity until we get
                // at least enough to hold the request, or we
                // hit the max blocksize whichever comes first
            }
        }
        // allocate and initialize a new block that will be the
        // tail of our interesting blocks
        bbBlock newblock = new bbBlock(newblocksize);
        newblock._idx = curr._idx + 1;
        newblock._offset = curr._offset + curr._limit; // we'll adjust this later like any existing block
        return newblock;
    }
    private int insertAsManyBlocksAsNeeded(Object caller, int version, bbBlock curr, int pos, int len, bbBlock newLastBlock)
    {
        assert mutation_in_progress(caller, version);
        // DEBUG: int amountAllocated = 0;
        // DEBUG: int origPos = this._buf_position;
        // this is the case where the old tail is pushed entirely out of the
        // old block into a new trailing block and then as many whole new
        // blocks as needed (which maybe none) are inserted between these two
        bbBlock oldCurr = curr;
        int   oldPosition = curr.blockOffsetFromAbsolute(pos);
        int   oldBlockTail = curr._limit - oldPosition;
        int   newSpaceInCurr = curr.unusedBlockCapacity();
        // adjust the curr blocks limit
        curr._limit += newSpaceInCurr;
        // DEBUG: amountAllocated += newSpaceInCurr;
        int   newoffset = curr._offset + curr._limit;
        int   spaceNeededInMiddle = len - newSpaceInCurr - newLastBlock._buffer.length;
        int   addedblocks = 0;
        bbBlock newblock = null;
        assert (spaceNeededInMiddle > 0);  // this is the "as many as needed" case not "this and next"
        // add blocks until we're ready for the last block
        while (spaceNeededInMiddle > 0) {
            addedblocks++;
            newblock = new bbBlock(this.nextBlockSize(caller, version));
            newblock._limit = newblock._buffer.length;
            if (newblock._limit > spaceNeededInMiddle) newblock._limit = spaceNeededInMiddle;
            // DEBUG: amountAllocated += newblock._limit;
            newblock._idx = curr._idx + addedblocks;
            newblock._offset = newoffset;
            this._blocks.add(newblock._idx, newblock);
            spaceNeededInMiddle -= newblock._limit;
            newoffset += newblock._limit;
        }
        // add the last block
        addedblocks++;
        newblock = newLastBlock;
        newblock._limit = newblock._buffer.length;
        // DEBUG: amountAllocated += newblock._limit;
        newblock._idx = curr._idx + addedblocks;
        newblock._offset = newoffset;
        this._blocks.add(newblock._idx, newblock);
        // DEBUG: assert (amountAllocated == len);
        // now adjust the trailing blocks
        adjustOffsets(newblock._idx, len, addedblocks);
        notifyInsert(pos, len);
        // now we copy the tail of the _curr block to the end of the space
        // note that this only works because the tail is being copied to
        // an altogether different block in the buffer, so it can't overlap
        if (oldBlockTail > 0) {
            System.arraycopy(oldCurr._buffer, oldPosition, newLastBlock._buffer, newLastBlock._limit - oldBlockTail, oldBlockTail);
        }
        // DEBUG: assert this.position() == origPos;
        // DEBUG: assert (amountAllocated == len);
        return len;
    }
    private void insertBlock(bbBlock newblock) {
        // in both cases we need to insert the new block after _curr
        // and adjust the idx values to go with that
        this._blocks.add(newblock._idx, newblock);
        _next_block_position++;
        for (int ii=newblock._idx + 1; ii < this._next_block_position; ii++) {
            this._blocks.get(ii)._idx++;
        }
    }
    private void adjustOffsets(int lastidx, int addedBytes, int addedBlocks) {
        bbBlock b;
        // now we adjust the trailing offsets
        if (addedBytes != 0 || addedBlocks != 0) {
            this._next_block_position += addedBlocks;
            for (int ii=lastidx + 1; ii < this._next_block_position; ii++) {
                b = this._blocks.get(ii);
                b._offset += addedBytes;
                b._idx += addedBlocks;
            }
            this._buf_limit += addedBytes;
        }
    }
    bbBlock remove(Object caller, int version, bbBlock curr, int pos, int len)
    {
        assert mutation_in_progress(caller, version);
        if (len == 0) return curr;
        if (len < 0 || (pos + len) > this._buf_limit) {
            throw new IllegalArgumentException();
        }
        int     amountToRemove = len;
        int     removedBlocks = 0;
        int     startingIdx = curr._idx;
        int     currIdx = curr._idx;
        bbBlock currBlock = curr;
        assert (curr._offset <= pos);
        assert (pos - curr._offset <= curr._limit);
        assert _validate();
        // this is to simply eliminate a big edge case
        if (pos == 0 && len == this._buf_limit) {
            this.clear(caller, version);
            notifyRemove(0, len);
            return null;
        }
        // remove from the initial block
        int currBlockPosition = currBlock.blockOffsetFromAbsolute(pos);
        int removedFromThisBlock = currBlock._limit - currBlockPosition;
        if (removedFromThisBlock > amountToRemove) removedFromThisBlock = amountToRemove;
        if (removedFromThisBlock == currBlock._limit) {
            // we'll be removing the whole block in the whole block loop below
            startingIdx--;  // so we have to back up on to fix the next block that will
                            // "fall" down into the soon to be emptied slot here
        }
        else {
            // we always copy into position, and we copy whatever is still
            // left in the end of the block
            int moveAmount = currBlock._limit - currBlockPosition - removedFromThisBlock;
            if (moveAmount > 0) {
                System.arraycopy(currBlock._buffer, currBlock._limit - moveAmount
                                ,currBlock._buffer, currBlockPosition, moveAmount);
            }
            amountToRemove -= removedFromThisBlock;
            currBlock._limit -= removedFromThisBlock;
            if (amountToRemove > 0) {
                // when we're on the last block, there'll be nothing to remove,
                // and no block to get either
                currIdx = currBlock._idx + 1;
                currBlock = this._blocks.get(currIdx);
            }
        }
        while (amountToRemove > 0 && amountToRemove >= currBlock._limit) {
            amountToRemove -= currBlock._limit;
            // remove the whole block - so first hang onto a reference
            bbBlock temp = currBlock;
            this._blocks.remove(currIdx);
            removedBlocks++;
            temp.clearBlock();
            this._blocks.add(temp); // dump it at the end (marked as not in use)
            // and we don't move currIdx because we bumped it out of the whole array
            if (currIdx < this._next_block_position - removedBlocks) {
                currBlock = this._blocks.get(currIdx);
            }
            else if (currIdx > 0) {
                currIdx--;
                currBlock = this._blocks.get(currIdx);
            }
            else {
                throw new BlockedBufferException("fatal - no current block!");
            }
        }
        if (amountToRemove > 0) {
            assert amountToRemove < currBlock._limit;
            System.arraycopy(currBlock._buffer, amountToRemove
                            ,currBlock._buffer, 0, currBlock._limit - amountToRemove);
            assert amountToRemove < currBlock._limit;
            currBlock._limit -= amountToRemove;
            currBlock._offset += amountToRemove;
        }
        // we'll even adjust the offset of the first block (if it's the last as well)
        adjustOffsets(startingIdx, -len, -removedBlocks);
        notifyRemove(pos, len);
        // DEBUG: int shouldBe = 0;
        // DEBUG: int is = currBlock._offset;
        // DEBUG: if (currBlock._idx > 0) {
        // DEBUG:     shouldBe = this._blocks.get(currBlock._idx - 1)._offset + this._blocks.get(currBlock._idx - 1)._limit;
        // DEBUG:     if (currIdx != startingIdx) assert (shouldBe == this._buf_position);
        // DEBUG: }
        // DEBUG: int delta = shouldBe - is;
        // DEBUG: assert(delta == 0);
        assert _validate();
        return currBlock;
    }
    static int _validate_count;
    public boolean _validate() {
        int pos = 0;
        int idx;
        boolean err = false;
        _validate_count++;
        if ((_validate_count % 128) != 0) return true;
        // you can change the 0 below (in from of the -2) to be the validation counter
        // which reported the failure and the test will be true when _validate() is
        // called on the last GOOD check.
        if (_validate_count == 30 -2) {
            // used to set breakpoints on particular calls for validation
            err = (_validate_count < 0);
        }
        for (idx=0; idx<this._blocks.size(); idx++) {
            bbBlock b = this._blocks.get(idx);
            if (b._idx == -1) break;
            if (b._idx != idx) {
                System.out.println("block "+idx+": index is wrong" +
                                   ", it is "+b._idx+
                                   " it should be "+idx);
                err = true;
            }
            if (b._offset != pos) {
                System.out.println("block "+idx+": starting offset is wrong"+
                                   ", it is "+b._offset+
                                   " should be "+pos);
                err = true;
            }
            // else because there's no point in using a bad reserved value to check the limit
            else if (b._limit < 0 || b._limit > b._buffer.length /* - b._reserved */ ) {
                System.out.println("block "+idx+": "+
                                   "limit is out of range"+
                                   ", it is "+b._limit+
                                   " should be between 0 and "+ (b._buffer.length /* - b._reserved */));
                err = true;
            }
            else if (b._limit == 0) {
                if ( ! (b._idx == (this._next_block_position - 1)
                     && b._offset == this._buf_limit)
                ) {
                    System.out.println("block "+idx+": "+
                                       "has a ZERO limit");
                    err = true;
                }
            }
            pos += b._limit;
        }
        if (idx != this._next_block_position) {
            System.out.println("next block position is wrong, is "+this._next_block_position+" should be "+idx);
            err = true;
        }
        for (idx++; idx<this._blocks.size(); idx++) {
            bbBlock b = this._blocks.get(idx);
            if (b._offset != -1) {
                System.out.println("block "+idx+": (in freed range) has non -1 offset, offset is "+b._offset);
                err = true;
            }
        }
        if (pos != this._buf_limit) {
            System.out.println("buffer _buf_limit: "+
                               "limit is incorrect"+
                               ", it is "+this._buf_limit+
                               " should be "+ pos);
            err = true;
        }
        if (this._next_block_position > 0) {
            bbBlock last = this._blocks.get(this._next_block_position - 1);
            if (last._offset + last._limit != this._buf_limit){
                System.out.println("last block "+last._idx+" limit isn't "+
                                   "_buf_limit ("+this._buf_limit+"): "+
                                   " calc'd last block limit is "
                                   +       last._offset +" + "+ last._limit
                                   +" = "+(last._offset + last._limit)
                                   );
                err = true;
            }
        }
        if (this._buf_limit < 0 || (this._buf_limit > 0 && this._next_block_position < 1)){
            System.out.println("this._buf_limit "+ this._buf_limit+ " is invalid");
            err = true;
        }
        if (err == true) {
            System.out.println("failed with validation count = " + _validate_count);
        }
        return err == false;  // validate is true if all is ok so that assert _validate(); works as expected
    }
    final static class bbBlock {
        public int     _idx;
        public int     _offset;
        public int     _limit;
        public byte[]  _buffer;
        public bbBlock(int capacity) {
            _buffer = new byte[capacity];
        }
        /**
         * Assumes ownership of an array to create a new block.  The data
         * within the buffer is maintained.
         *
         * @param buffer contains the data for the block.
         *
         * @throws NullPointerException if buffer is null.
         */
        bbBlock(byte[] buffer) {
            _buffer = buffer;
            _limit = buffer.length;
        }
        public bbBlock clearBlock() {
            _idx = -1;
            _offset = -1;
            _limit = 0;
            return this;
        }
        /**
         * maximimum number of bytes that can be held in this block.
         */
        final int blockCapacity() {
            assert this._offset >= 0;
            return this._buffer.length ;
        }
        /**
         * maximimum number of bytes that can be appended in this block currently.
         */
        final int unusedBlockCapacity() {
            assert this._offset >= 0;
            return this._buffer.length - this._limit;
        }
        /**
         * Gets the number of bytes between the current position and the
         * writable capacity of this block.
         * @param pos absolute position
         */
        final int bytesAvailableToWrite(int pos) {
            assert this._offset >= 0;
            return this._buffer.length - (pos - _offset);
        }
        /**
         * Gets the number of, as yet, unused bytes in this block.  That's the number
         * of bytes that can be inserted into this block without overflowing, or the
         * number of bytes between the current position and the end of the written bytes
         * in this block
         * @param pos absolute position
         */
        public final int bytesAvailableToRead(int pos) {
            assert this._offset >= 0;
            return this._limit - (pos - _offset);
        }
        /**
         * is there space between position and capacity?
         * @param pos absolute position
         * @param needed
         * @return boolean
         */
        final boolean hasRoomToWrite(int pos, int needed) {
            assert this._offset >= 0;
            return (needed <= (this._buffer.length - (pos - _offset)));
        }
        final boolean containsForRead(int pos) {
            assert this._offset >= 0;
            return (pos >= _offset && pos < _offset + _limit);
        }
        final boolean containsForWrite(int pos) {
            assert this._offset >= 0;
            return (pos >= _offset && pos <= _offset + _limit);
        }
        final int blockOffsetFromAbsolute(int pos) {
            assert this._offset >= 0;
            return pos - _offset;
        }
    }
    public interface Monitor
    {
        public boolean notifyInsert(int pos, int len);
        public boolean notifyRemove(int pos, int len);
        public int     getMemberIdOffset();
    }
    private final static class PositionMonitor implements Monitor
    {
        int _pos;
        PositionMonitor(int pos) { _pos = pos; }
        public int getMemberIdOffset() { return _pos; }
        public boolean notifyInsert(int pos, int len) { return false; }
        public boolean notifyRemove(int pos, int len) { return false; }
    }
    private final static class CompareMonitor implements Comparator<Monitor> {
        static CompareMonitor instance = new CompareMonitor();
        private CompareMonitor() {}
        static CompareMonitor getComparator()
        {
            return instance;
        }
        public int compare(Monitor arg0, Monitor arg1)
        {
            return arg0.getMemberIdOffset() - arg1.getMemberIdOffset();
        }
    }
    TreeSet<Monitor> _updatelist = new TreeSet<Monitor>(CompareMonitor.getComparator());
    public void notifyRegister(Monitor item) {
        _updatelist.add(item);
    }
    public void notifyUnregister(Monitor item) {
        _updatelist.remove(item);
    }
    public void notifyInsert(int pos, int len) {
        if (len == 0) return;
        PositionMonitor pm = new PositionMonitor(pos);
        SortedSet<Monitor> follows = _updatelist.tailSet(pm);
        for (Monitor m : follows) {
            if (m.notifyInsert(pos, len)) {
                follows.remove(m);
            }
        }
    }
    public void notifyRemove(int pos, int len) {
        if (len == 0) return;
        PositionMonitor pm = new PositionMonitor(pos);
        SortedSet<Monitor> follows = _updatelist.tailSet(pm);
        for (Monitor m : follows) {
            if (m.notifyRemove(pos, len)) {
                follows.remove(m);
            }
        }
    }
    /**
     * Reads data from a byte buffer, keeps a local position and
     * a current block.  Snaps a buffer length on creation;
     */
    public static class BlockedByteInputStream extends java.io.InputStream
    {
        BlockedBuffer _buf;
        int           _pos;
        int           _mark;
        bbBlock       _curr;
        int           _blockPosition;
        int           _version;
        /**
         * @param bb blocked buffer to read from
         */
        public BlockedByteInputStream(BlockedBuffer bb)
        {
            this(0, bb);
        }
        /**
         * @param bb blocked buffer to read from
         * @param pos initial offset to read
         */
        public BlockedByteInputStream(BlockedBuffer bb, int pos)
        {
            this(pos, bb);
        }
        /**
         * @param pos initial offset to read
         * @param end is the local limit, or -1 (_end_unspecified)
         * @param bb blocked buffer to read from
          */
        private BlockedByteInputStream(int pos, BlockedBuffer bb)
        {
            if (bb == null) throw new IllegalArgumentException();
            _version = bb.getVersion();
            _buf = bb;
            _set_position(pos);
            _mark = -1;
        }
        @Override
        public final void mark(int readlimit) {
            this._mark = this._pos;
        }
        @Override
        public final void reset() throws IOException {
            if (this._mark == -1) throw new IOException("mark not set");
            _set_position(this._mark);
        }
        /**
         * the current offset in the buffer
         */
        public final int position() {
            return this._pos;
        }
        /**
         * this forces a version sync with the underlying blocked buffer.
         * The current position is lost during this call.
         *
         */
        public final void sync() throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _version = _buf.getVersion();
            _curr = null;
            _pos = 0;
        }
        /**
         * debug api to force check for internal validity of the
         * underlying buffer
         */
        public final boolean _validate() {
            return this._buf._validate();
        }
        /**
         * sets the position of the stream to be pos. The next operation
         * (such as read) will return the byte at that offset.
         * @param pos new offset to read from
         * @return this stream
         */
        public final BlockedByteInputStream setPosition(int pos) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (pos < 0 || pos > _buf.size()) {
                throw new IllegalArgumentException();
            }
            // call our unfailing private method to do the real work
            _set_position(pos);
            fail_on_version_change();
            return this;
        }
        private final void _set_position(int pos)
        {
            _pos = pos;
            _curr = _buf.findBlockForRead(this, _version, _curr, pos);
            _blockPosition = _pos - _curr._offset;
        }
        /**
         * closes the steam and clears its reference to the
         * byte buffer.  Once closed it cannot be used.
         */
        @Override
        public final void close() throws IOException
        {
            this._buf = null;
            this._pos = -1;
        }
        public final int writeTo(OutputStream out, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (_pos > _buf.size()) throw new IllegalArgumentException();

            int startingPos = _pos;
            int localEnd = _pos + len;
            if (localEnd > _buf.size()) localEnd = _buf.size();
            assert(_curr.blockOffsetFromAbsolute(_pos) == _blockPosition);

            while (_pos < localEnd) {
                int available = _curr._limit - _blockPosition;
                boolean partial_read = available > localEnd - _pos;
                if (partial_read) {
                    available = localEnd - _pos;
                }

                out.write(_curr._buffer, _blockPosition, available);
                _pos += available;
                if (partial_read) {
                    _blockPosition += available;
                    break;
                }
                _curr = _buf.findBlockForRead(this, _version, _curr, _pos);
                _blockPosition =_curr.blockOffsetFromAbsolute(_pos);
            }

            fail_on_version_change();
            return _pos - startingPos;
        }

        public final int writeTo(ByteWriter out, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (_pos > _buf.size()) throw new IllegalArgumentException();

            int startingPos = _pos;
            int localEnd = _pos + len;
            if (localEnd > _buf.size()) localEnd = _buf.size();
            assert(_curr.blockOffsetFromAbsolute(_pos) == _blockPosition);

            while (_pos < localEnd) {
                int available = _curr._limit - _blockPosition;
                boolean partial_read = available > localEnd - _pos;
                if (partial_read) {
                    available = localEnd - _pos;
                }
                out.write(_curr._buffer, _blockPosition, available);
                _pos += available;
                if (partial_read) {
                    _blockPosition += available;
                    break;
                }
                _curr = _buf.findBlockForRead(this, _version, _curr, _pos);
                _blockPosition =_curr.blockOffsetFromAbsolute(_pos);
            }

            fail_on_version_change();
            return _pos - startingPos;
        }

        /**
         * reads (up to) {@code len} bytes from the buffer and copies them into
         * the user supplied byte array (bytes) starting at offset
         * off in the users array.  This returns the number of bytes
         * read, which may be less than the number requested if
         * there is not enough data available in the buffer.
         *
         * @throws IndexOutOfBoundsException
         *   if {@code (dst.length - offset) < len}
         */
        @Override
        public final int read(byte[] bytes, int offset, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (_pos > _buf.size()) throw new IllegalArgumentException();
            int startingPos = _pos;
            int localEnd = _pos + len;
            if (localEnd > _buf.size()) localEnd = _buf.size();
            while (_pos < localEnd) {
                bbBlock block = _curr;
                int block_offset = _blockPosition;
                int available = block._limit - _blockPosition;
                if (available > localEnd - _pos) {
                    // we aren't emptying this block so adjust our location
                    available = localEnd - _pos;
                    _blockPosition += available;
                }
                else {
                    // TODO can't we just move to the next block?
                    _curr = _buf.findBlockForRead(this, _version, _curr, _pos + available);
                    _blockPosition = 0;
                }
                System.arraycopy(block._buffer, block_offset, bytes, offset, available);
                _pos += available;
                offset += available;
            }
            fail_on_version_change();
            return _pos - startingPos;
        }
        /**
         * reads the next byte in the buffer.  This returns -1
         * if there is no data available to be read.
         */
        @Override
        public final int read() throws IOException
        {
            if (_buf == null) {
                throw new IOException("input stream is closed");
            }
            fail_on_version_change();
            if (_pos >= _buf.size()) return -1;
            if (_blockPosition >= _curr._limit) {
                _curr = this._buf.findBlockForRead(this, _version, _curr, _pos);
                _blockPosition = 0;
            }
            int nextByte = (0xff & _curr._buffer[_blockPosition]);
            _blockPosition++;
            _pos++;
            fail_on_version_change();
            return nextByte;
        }
        private final void fail_on_version_change() throws IOException
        {
            if (_buf.getVersion() != _version) {
                this.close();
                throw new BlockedBufferException("buffer has been changed!");
            }
        }
        @Override
        public final long skip(long n) throws IOException
        {
            if (n < 0 || n > Integer.MAX_VALUE) throw new IllegalArgumentException("we only handle buffer less than "+Integer.MAX_VALUE+" bytes in length");
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (_pos >= _buf.size()) return -1;
            int len = (int)n;
            if (len == 0) return 0;
            int startingPos = _pos;
            int localEnd = _pos + len;
            if (localEnd > _buf.size()) localEnd = _buf.size();
            // if we run off the end of this block, we need to update
            // our current block ( _curr ) we'll update the block position
            // in any event (once we know the right block, of course)
            if (localEnd > _blockPosition + _curr._offset) {
                _curr = _buf.findBlockForRead(this, _version, _curr, localEnd);
            }
            _blockPosition = localEnd - _curr._offset;
            _pos = localEnd;
            fail_on_version_change();
            return _pos - startingPos;
        }
    }
    /**
     * Reads data from a byte buffer, keeps a local position and
     * a current block.  Snaps a buffer length on creation;
     */
    public static class BlockedByteOutputStream extends java.io.OutputStream
    {
        BlockedBuffer _buf;
        int           _pos;
        bbBlock       _curr;
        int           _blockPosition;
        int           _version;
        /**
         * creates writable stream (OutputStream) that writes
         * to a fresh blocked buffer.  The stream is initially
         * position at offset 0.
         */
        public BlockedByteOutputStream() {
            _buf = new BlockedBuffer();
            _version = _buf.getVersion();
            _set_position(0);
        }
        /**
         * creates writable stream (OutputStream) that writes
         * to the supplied byte buffer.  The stream is initially
         * position at offset 0.
         * @param bb blocked buffer to write to
         */
        public BlockedByteOutputStream(BlockedBuffer bb) {
            _buf = bb;
            _version = _buf.getVersion();
            _set_position(0);
        }
        /**
         * creates writable stream (OutputStream) that can write
         * to the supplied byte buffer.  The stream is initially
         * position at offset off.
         * @param bb blocked buffer to write to
         * @param off initial offset to write to
         */
        public BlockedByteOutputStream(BlockedBuffer bb, int off) {
            if (bb == null || off < 0 || off > bb.size() ) {
                throw new IllegalArgumentException();
            }
            _buf = bb;
            _version = _buf.getVersion();
            _set_position(0);
        }
        /**
         * the current offset in the buffer
         */
        public final int position() {
            return this._pos;
        }
        /**
         * this forces a version sync with the underlying blocked buffer.
         * The current position is lost during this call.
         *
         */
        public final void sync() throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _version = _buf.getVersion();
            _pos = 0;
            _curr = null;
        }
        /**
         * debug api to force check for internal validity of the
         * underlying buffer
         */
        public final boolean _validate() {
            return this._buf._validate();
        }
        /**
         * repositions this stream in the buffer.  The next
         * read, write, or insert operation will take place
         * at the specified position.  The position must
         * be within the contiguous range of written bytes,
         * including the pseudo end of file character just
         * past the end, which can be written on and returns
         * -1 if read.
         */
        public final BlockedByteOutputStream setPosition(int pos) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            fail_on_version_change();
            if (pos < 0 || pos > _buf.size()) {
                throw new IllegalArgumentException();
            }
            this._set_position(pos);
            fail_on_version_change();
            return this;
        }
        private final void _set_position(int pos)
        {
            _pos = pos;
            _curr = _buf.findBlockForRead(this, _version, _curr, pos);
            _blockPosition = _pos - _curr._offset;
            return;
        }
        /**
         * closes the steam and clears its reference to the
         * byte buffer.  Once closed it cannot be used.
         */
        @Override
        public final void close() throws IOException
        {
            this._buf = null;
            this._pos = -1;
            return;
        }
        /**
         * Inserts space and writes 1 byte to the current
         * position in this output stream.  Only the low
         * order byte of the passed in int is written the
         * high order bits are ignored.
         */
        @Override
        public final void write(int b) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _write(b);
            _version = _buf.end_mutate(this);
            return;
        }
        final void start_write() {
            _buf.start_mutate(this, _version);
        }
        final void end_write() {
            _version = _buf.end_mutate(this);
        }
        final void _write(int b) throws IOException
        {
            if (bytesAvailableToWriteInCurr(_pos) < 1) {
                _curr = _buf.findBlockForWrite(this, _version, _curr, _pos);
                assert _curr._offset == _pos;
                _blockPosition = 0;
            }
            _curr._buffer[_blockPosition++] = (byte)(b & 0xff);
            _pos++;
            if (_blockPosition > _curr._limit) {
                _curr._limit = _blockPosition;
                if (_pos > _buf._buf_limit ) _buf._buf_limit = _pos;
            }
        }
        private final int bytesAvailableToWriteInCurr(int pos) {
            assert _curr != null;
            assert _curr._offset <= pos;
            assert _curr._offset + _curr._limit >= pos;
            if (_curr._idx < this._buf._next_block_position - 1) {
                return _curr.bytesAvailableToRead(pos);
            }
            int ret = _curr._buffer.length - (pos - _curr._offset);
            return ret; // _curr.bytesAvailableToWrite(pos);
        }
        /**
         * Writes len bytes from the specified byte array starting
         * at in the user array at offset off to the current position
         * in this output stream.
         */
        @Override
        public final void write(byte[] b, int off, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _write(b, off, len);
            _version = _buf.end_mutate(this);
        }
        private final void _write(byte[] b, int off, int len)
        {
            int end_b = off + len;
            while (off < end_b)
            {
                int writeInThisBlock = bytesAvailableToWriteInCurr(_pos);
                if (writeInThisBlock > end_b - off) {
                    writeInThisBlock = end_b - off;
                }
                assert writeInThisBlock >= 0;
                if (writeInThisBlock > 0) {
                    System.arraycopy(b, off, _curr._buffer, _blockPosition, writeInThisBlock);
                    off += writeInThisBlock;
                    _pos += writeInThisBlock;
                    _blockPosition += writeInThisBlock;
                    if (_blockPosition > _curr._limit) {
                        _curr._limit = _blockPosition;
                        if (_pos > _buf._buf_limit) _buf._buf_limit = _pos;
                    }
                    else {
                        assert _pos <= _buf._buf_limit;
                    }
                }
                if (off >= end_b) break;

                _curr = _buf.findBlockForWrite(this, _version, _curr, _pos);
                _blockPosition = _curr.blockOffsetFromAbsolute(_pos);
                assert _curr._offset == _pos || off >= end_b;
            }
        }
        /**
         * Writes bytes from the specified byte stream from its current
         * stream position to the end of the stream.  Writing the bytes
         * to the current position in this output stream.
         * @throws IOException
         */
        public final void write(InputStream bytestream) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _write(bytestream, -1);
            _version = _buf.end_mutate(this);
        }
        /**
         * Writes bytes from the specified byte stream from its current
         * stream position up to length bytes from the stream.  Writing the
         * bytes to the current position in this output stream.
         * @throws IOException
         */
        public final void write(InputStream bytestream, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _write(bytestream, len);
            _version = _buf.end_mutate(this);
        }
        /**
         * helper to write data.  This does not check input arguments.
         * @param bytestream source of the data
         * @param len number of bytes to read from the input stream, -1 for all
         * @throws IOException
         */
        private final void _write(InputStream bytestream, int len) throws IOException
        {
            if (len == 0) return;

            int written = 0;
            boolean read_all = (len == -1);

            for (;;)
            {
                int writeInThisBlock = bytesAvailableToWriteInCurr(_pos);
                assert writeInThisBlock >= 0;

                int to_read = read_all ? writeInThisBlock : len;
                if (to_read > writeInThisBlock) {
                    to_read = writeInThisBlock;
                }
                int len_read = bytestream.read(_curr._buffer, _blockPosition, to_read);
                if (len_read == -1) break;
                if (len_read > 0) {
                    _pos += len_read;
                    _blockPosition += len_read;
                    if (_blockPosition > _curr._limit) {
                        _curr._limit = _blockPosition;
                        if (_pos > _buf._buf_limit) _buf._buf_limit = _pos;
                    }
                    else {
                        assert _pos <= _buf._buf_limit;
                    }
                }

                if (len_read == writeInThisBlock) {
                    _curr = _buf.findBlockForWrite(this, _version, _curr, _pos);
                    _blockPosition = _curr.blockOffsetFromAbsolute(_pos);
                    assert _curr._offset == _pos || written < len_read;
                }
                else {
                    assert len_read < writeInThisBlock;
                }
                if (!read_all) {
                    len -= len_read;
                    if (len < 1) break;
                }
            }
        }
        /**
         * Inserts the amount space requested at the current
         * position in this output stream.  No data is written
         * into the output stream.
         */
        public final void insert(int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            if (len < 0) {
                throw new IllegalArgumentException();
            }
            if (len > 0) {
                _buf.start_mutate(this, _version);
                _buf.insert(this, _version, _curr, _pos, len);
                _version = _buf.end_mutate(this);
            }
            return;
        }
        /**
         * Inserts space and writes 1 byte to the current
         * position in this output stream.  Only the low
         * order byte of the passed in int is written the
         * high order bits are ignored.
         */
        public final void insert(byte b) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _buf.insert(this, _version, _curr, _pos, 1);
            _write(b);
            _version = _buf.end_mutate(this);
        }
        /**
         * Inserts space and writes len bytes from the specified
         * byte array starting at in the user array at offset off
         * to the current position in this output stream.
         */
        public final void insert(byte[] b, int off, int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _buf.insert(this, _version, _curr, _pos, len);
            _write(b, off, len);
            _version = _buf.end_mutate(this);
        }
        /**
         * Inserts space and writes len bytes from the specified
         * byte array starting at in the user array at offset off
         * to the current position in this output stream.
         */
        public final void remove(int len) throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            _buf.start_mutate(this, _version);
            _curr = _buf.remove(this, _version, _curr, _pos, len);
            _version = _buf.end_mutate(this);
        }
        /**
         * trucates the buffer at the current location after this
         * call the last previously written or read byte will be
         * the end of the buffer.
         */
        public final void truncate() throws IOException
        {
            if (_buf == null) throw new IOException("stream is closed");
            if (this._buf._buf_limit == _pos) return;
            _buf.start_mutate(this, _version);
            _curr = _buf.truncate(this, _version, _pos);
            _version = _buf.end_mutate(this);
        }
        private final void fail_on_version_change() throws IOException
        {
            if (_buf.getVersion() != _version) {
                this.close();
                throw new BlockedBufferException("buffer has been changed!");
            }
        }
    }
    public static class BlockedBufferException extends IonException
    {
        private static final long serialVersionUID = 1582507845614969389L;
        public BlockedBufferException() { super(); }
        public BlockedBufferException(String message) { super(message); }
        public BlockedBufferException(String message, Throwable cause) {
            super(message, cause);
        }
        public BlockedBufferException(Throwable cause) { super(cause); }
    }
    public static class BufferedOutputStream
        extends OutputStream
    {
        BlockedBuffer           _buffer;
        BlockedByteOutputStream _writer;
        public BufferedOutputStream() {
            this(new BlockedBuffer());
        }
        public BufferedOutputStream(BlockedBuffer buffer) {
            _buffer = buffer;
            _writer = new BlockedByteOutputStream(_buffer);
        }
        /**
         * Gets the size in bytes of this binary data.
         * This is generally needed before calling {@link #getBytes()} or
         * {@link #getBytes(byte[], int, int)}.
         *
         * @return the size in bytes.
         */
        public int byteSize()
        {
            return _buffer.size();
        }
        /**
         * Copies the current contents of this writer as a new byte array holding
         * Ion binary-encoded data.
         * This allocates an array of the size needed to exactly
         * hold the output and copies the entire value to it.
         *
         * @return the byte array with the writers output
         * @throws IOException
         */
        public byte[] getBytes()
            throws IOException
        {
            int size = byteSize();
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream(size);
            writeBytes(byteStream);
            byte[] bytes = byteStream.toByteArray();
            return bytes;
        }
        /**
         * Copies the current contents of the writer to a given byte array
         * array.  This starts writing to the array at offset and writes
         * up to len bytes.
         * If this writer is not able to stop in the middle of its
         * work this may overwrite the array and later throw and exception.
         *
         * @param bytes users byte array to write into
         * @param offset initial offset in the array to write into
         * @param len maximum number of bytes to write from offset on
         * @return number of bytes written
         * @throws IOException
         */
        public int getBytes(byte[] bytes, int offset, int len)
            throws IOException
        {
            SimpleByteBuffer outbuf = new SimpleByteBuffer(bytes, offset, len);
            OutputStream     writer = (OutputStream)outbuf.getWriter();
            int              written = writeBytes(writer);
            return written;
        }
        /**
         * Writes the current contents of the writer to the output
         * stream.  This is only valid if the writer is not in the
         * middle of writing a container.
         *
         * @param userstream OutputStream to write the bytes to
         * @return int length of bytes written
         * @throws IOException
         */
        public int writeBytes(OutputStream userstream)
            throws IOException
        {
            int limit = _buffer.size();
            int pos = 0;
            int version = _buffer.getVersion();
            bbBlock curr = null;
            _buffer.start_mutate(this, version);
            while (pos < limit) {
                curr = _buffer.findBlockForRead(this, version, curr, pos);
                if (curr == null) {
                    throw new IOException("buffer missing expected bytes");
                }
                int len = curr.bytesAvailableToRead(pos);
                if (len <= 0) {
                    throw new IOException("buffer missing expected bytes");
                }
                userstream.write(curr._buffer, 0, len);
                pos += len;
            }
            _buffer.end_mutate(this);
            return pos;
        }
        @Override
        public void write(int b) throws IOException
        {
            _writer.write(b);
        }
        @Override
        public void write(byte[] bytes) throws IOException
        {
            write(bytes, 0, bytes.length);
        }
        @Override
        public void write(byte[] bytes, int off, int len) throws IOException
        {
            _writer.write(bytes, off, len);
        }
    }
}
