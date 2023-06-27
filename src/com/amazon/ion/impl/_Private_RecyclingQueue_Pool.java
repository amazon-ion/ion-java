package com.amazon.ion.impl;

import com.amazon.ion.impl.bin.utf8.Pool;

// The _Private_RecyclingQueue
public class _Private_RecyclingQueue_Pool extends Pool<_Private_RecyclingQueue<PatchPoint>>{
    private static final _Private_RecyclingQueue_Pool INSTANCE = new _Private_RecyclingQueue_Pool();

    private _Private_RecyclingQueue_Pool() {
        super(new Allocator<_Private_RecyclingQueue<PatchPoint>>() {
            @Override
            public _Private_RecyclingQueue<PatchPoint> newInstance(Pool<_Private_RecyclingQueue<PatchPoint>> pool) {
                return new _Private_RecyclingQueue(pool);
            }
        });
    }

    public  static _Private_RecyclingQueue_Pool getInstance() {
        return INSTANCE;
    }
}
