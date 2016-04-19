/*
 * Copyright 2009-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;


/**
 *   These classes (UnifiedSavePointManager and the contained
 *   SavePoint) isolate the save point handing. Its effect spans
 *   both the input stream and the underlying buffer - so the manager
 *   keeps it's own reference to these.  By doing so this should
 *   make save points easier to use.  It grabs the buffer from
 *   the stream since it need a reference to the UnifiedInputBuffer
 *   on most calls and doesn't need an extra de-ref (mostly for
 *   code clarity)
 *
 *   The general life time of a save point is:
 *   . allocate a save point
 *   . . start the save point, which sets the start idx and pos - and
 *       pins the buffer pages with a use count
 *   . . mark_end the save point, which sets the end idx and pos
 *   . . . activate the save point, which sets the input streams pos
 *         to the start
 *   . . . deactivate the save point, which pops the save point stack
 *         and restores the stream to its previous position
 *   . . clear the save point. this clears its internal values and
 *       releases its use counter on the buffer. As the buffers in use
 *       count goes to zero it may release any unnecessary pages.
 *   . free the save point
 *
 *   this order of operation, as nested above, is enforced by checking
 *   the state of the save points and member values like the end position.
 *
 *   if the end position is not set it is treated as "to end of file"
 *
 */
final class UnifiedSavePointManagerX
{
    private static final int FREE_LIST_LIMIT = 20;

    UnifiedInputStreamX  _stream;
    UnifiedInputBufferX  _buffer;
    SavePoint           _inuse;
    SavePoint           _free;
    int                 _free_count;
    SavePoint           _active_stack;
    int                 _open_save_points;

    public UnifiedSavePointManagerX(UnifiedInputStreamX  stream) {
        _stream = stream;
        _buffer = stream._buffer;
        _inuse = null;
        _free = null;
        _active_stack = null;
    }

    public final boolean isSavePointOpen() {
        return (_open_save_points > 0);
    }

    public final long lengthOf(SavePoint sp)
    {
        int start_idx = sp.getStartIdx();
        int end_idx   = sp.getEndIdx();

        if (start_idx == -1 || end_idx == -1) {
            return 0;
        }

        long len;
        if (start_idx == end_idx) {  // a very common case
            int start_pos = sp.getStartPos();
            int end_pos   = sp.getEndPos();
            len = end_pos - start_pos;
        }
        else {
            UnifiedDataPageX start = _buffer.getPage(start_idx);
            UnifiedDataPageX end   = _buffer.getPage(end_idx);
            long start_pos = start.getFilePosition(sp.getStartPos());
            long end_pos   = end.getFilePosition(sp.getEndPos());
            len = end_pos - start_pos;
        }
        return len;
    }

    public final SavePoint savePointAllocate() {
        SavePoint sp;
        if (_free != null) {
            sp = _free;
            _free = sp._next;
            _free_count--;
            sp.clear();
        }
        else {
            sp = new SavePoint(this);
        }
        sp._next = _inuse;
        sp._prev = null;
        if (_inuse != null) {
            _inuse._prev = sp;
        }
        else {
            _inuse = sp;
        }
        return sp;
    }
    public final void savePointFree(SavePoint sp)
    {
        assert(sp.isClear());

        if (_free_count >= FREE_LIST_LIMIT) {
            // by not putting this on the free list
            // the GC is free to clean it up.
            return;
        }

        if (sp._prev == null) {
            sp._prev = sp._next;
        }
        else {
            _inuse = sp._next;
        }
        if (sp._next  != null) {
            sp._next._prev = sp._prev;
        }
        sp._next = _free;
        _free = sp;
        _free_count++;
    }
    public final SavePoint savePointActiveTop() {
        return _active_stack;
    }

    public final void savePointPushActive(SavePoint sp, long line_number, long line_start) {
        assert(!sp.isActive());

        int      idx = _buffer.getCurrentPageIdx();
        int      pos = _stream._pos;
        int      limit = _stream._limit;
        UnifiedDataPageX curr = _buffer.getPage(idx);

        // save our current state in the Save Point so when
        // we pop it off we can restore our current state
        sp.set_prev_pos(idx, pos, limit, line_number, line_start);

        // actually push this save point on the stack
        sp._next_active = _active_stack;
        _active_stack = sp;
        sp.set_active();

        // if the start page is also the last page we
        // need set the limit to the sp end, otherwise
        // we use the limit from the page and we'll
        // deal with the last page when we get to it
        idx = sp.getStartIdx();
        pos = sp.getStartPos();
        curr = _buffer.getPage(idx);
        if (sp.getEndIdx() != sp.getStartIdx()) {
            limit = curr.getBufferLimit();
        }
        else {
            limit = sp.getEndPos();
        }
        _stream.make_page_current(curr, idx, pos, limit);
    }

    public final void savePointPopActive(SavePoint sp)
    {
        if (sp != _active_stack) {
            throw new IllegalArgumentException("save point being released isn't currently active");
        }

        _active_stack = sp._next_active;
        sp._next_active = null;
        sp.set_inactive();

        _stream.save_point_reset_to_prev(sp);

        return;
    }

    private void save_point_clear(SavePoint sp)
    {
        if (sp.isClear()) {
            return;
        }
        int start_idx = sp.getStartIdx();
        int end_idx = sp.getEndIdx();
        if (end_idx != -1 || start_idx != -1) {
            if (start_idx != -1) {
                _open_save_points--;
                save_point_unpin(sp);
            }
        }
    }
    private final void save_point_unpin(SavePoint sp) {
        if (sp.isActive()) {
            throw new IllegalArgumentException("you can't release an active save point");
        }
        assert(sp.isDefined());

        if (_buffer.decLock()) {
            if (_open_save_points == 0) {
                _buffer.resetToCurrentPage();
            }
        }
        return;
    }
    private final SavePoint save_point_start(SavePoint sp, long line_number, long line_start) {
        if (sp.isDefined()) {
            throw new IllegalArgumentException("you can't start an active save point");
        }

        int new_pinned_idx = _buffer.getCurrentPageIdx();
        _buffer.incLock();
        sp.set_start_pos(new_pinned_idx,_stream._pos, line_number, line_start);
        _open_save_points++;

        return sp;
    }
    private final void save_point_mark_end(SavePoint sp, int offset) {
        if (sp.isActive()) {
            throw new IllegalArgumentException("you can't start an active save point");
        }

        UnifiedDataPageX curr = _buffer.getCurrentPage();
        int curr_idx = _buffer.getCurrentPageIdx();
        int curr_pos = _stream._pos + offset;

        // this adjusts the current page idx and pos (in the page buffer)
        // to handle the end point being offset from the current pos
        // since that may result in the end mark referencing a different page
        if (offset != 0) {
            if (curr_pos >= curr.getBufferLimit()) {
                curr_pos -= curr.getOriginalStartingOffset();
                curr_idx++;
                curr = _buffer.getPage(curr_idx);
            }
            else if (curr_pos < curr.getStartingOffset()) {
                int pos_offset = curr_pos - curr.getOriginalStartingOffset();
                curr_idx--;
                curr = _buffer.getPage(curr_idx);
                curr_pos = curr.getBufferLimit() - pos_offset;
            }
            if (curr == null || curr_pos >= curr.getBufferLimit() || curr_pos < curr.getStartingOffset()) {
                end_point_too_far(curr_idx);
            }
        }

        sp.set_end_pos(curr_idx, curr_pos); // we may be "re-setting" the idx (that's ok)

        return;
    }
    private final void end_point_too_far(int curr_idx) {
        String message = "end point ["
                       + curr_idx
                       + "] must be within 1 page of current ["
                       + _buffer.getCurrentPageIdx()
                       + "]";
        throw new IllegalArgumentException(message);
    }

    public static class SavePoint
    {
        public enum SavePointState { CLEAR, DEFINED, ACTIVE }

        private UnifiedSavePointManagerX _owner;
        private SavePointState   _state;
        private int              _start_idx, _start_pos;
        private long             _start_line_count;
        private long             _start_line_start;
        private int              _end_idx, _end_pos;
        private int              _prev_idx, _prev_pos, _prev_limit;
        private long             _prev_line_count;
        private long             _prev_line_start;
        private SavePoint        _next, _prev;
        private SavePoint        _next_active;

        SavePoint(UnifiedSavePointManagerX owner) {
            clear();
            _owner = owner;
        }
        private final void set_start_pos(int idx, int pos, long line_count, long line_start) {
            assert(_state == SavePointState.CLEAR);
            _state = SavePointState.DEFINED;
            _start_idx = idx;
            _start_pos = pos;
            _start_line_count = line_count;
            _start_line_start = line_start;
        }
        private final void set_end_pos(int idx, int pos) {
            assert(_state == SavePointState.DEFINED);
            _end_idx = idx;
            _end_pos = pos;
        }
        private final void set_prev_pos(int idx, int pos, int limit, long line_count, long line_start) {
            assert(_state == SavePointState.DEFINED);
            _prev_idx = idx;
            _prev_pos = pos;
            _prev_limit = limit;
            _prev_line_count = line_count;
            _prev_line_start = line_start;
        }

        public final void clear() {
            assert(_state != SavePointState.ACTIVE);
            if (isDefined()) {
                _owner.save_point_clear(this);
            }
            _state = SavePointState.CLEAR;
            _start_idx = -1;
            _end_idx = -1;
            _prev_idx = -1;
        }
        public final void start(long line_number, long line_start) {
            _owner.save_point_start(this, line_number, line_start);
        }
        public final void markEnd() {
            _owner.save_point_mark_end(this, 0);
        }
        public final void markEnd(int offset) {
            _owner.save_point_mark_end(this, offset);
        }
        public final void free() {
            _owner.savePointFree(this);
        }
        public final boolean isClear() {
            return (_state == SavePointState.CLEAR);
        }
        public final boolean isDefined() {
            return (_state == SavePointState.DEFINED || _state == SavePointState.ACTIVE);
        }
        public final boolean isActive() {
            return (_state == SavePointState.ACTIVE);
        }
        public final void set_active() {
            assert(_state == SavePointState.DEFINED);
            _state = SavePointState.ACTIVE;
        }
        public final void set_inactive() {
            assert(_state == SavePointState.ACTIVE);
            _state = SavePointState.DEFINED;
        }
        public final long length() {
            if (_start_idx == -1 || _end_idx == -1) {
                return 0;
            }
            return _owner.lengthOf(this);
        }

        public final int getStartIdx() {
            return _start_idx;
        }
        public final int getStartPos() {
            assert(_state != SavePointState.CLEAR);
            return _start_pos;
        }
        public final long getStartLineNumber() {
            return _start_line_count;
        }
        public final long getStartLineStart() {
            return _start_line_start;
        }
        public final long getStartFilePosition() {
            if (_start_idx == -1) return -1;
            UnifiedDataPageX p = _owner._buffer.getPage(_start_idx);
            return p.getFilePosition(_start_pos);
        }
        public final int getEndIdx() {
            return _end_idx;
        }
        public final int getEndPos() {
            assert(_state != SavePointState.CLEAR);
            return _end_pos;
        }
        public final long getEndFilePosition() {
            assert(_state != SavePointState.CLEAR);
            if (_end_idx == -1) return -1;
            UnifiedDataPageX p = _owner._buffer.getPage(_end_idx);
            return p.getFilePosition(_end_pos);
        }
        public final int getPrevIdx() {
            return _prev_idx;
        }
        public final int getPrevPos() {
            assert(_state != SavePointState.CLEAR);
            return _prev_pos;
        }
        public final int getPrevLimit() {
            assert(_state != SavePointState.CLEAR);
            return _prev_limit;
        }
        public final long getPrevLineNumber() {
            return _prev_line_count;
        }
        public final long getPrevLineStart() {
            return _prev_line_start;
        }
    }
}

