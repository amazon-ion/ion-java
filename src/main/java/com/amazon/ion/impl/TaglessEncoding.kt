// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

/**
 * The tagless encodings supported by Ion 1.1+.
 *
 * TODO:
 *   - Consider moving to [com.amazon.ion.impl.macro].
 *   - Try to make this `internal` or `package-private`
 */
enum class TaglessEncoding(@JvmField internal val typeID: IonTypeID, @JvmField val isUnsigned: Boolean) {
    UINT8(IonTypeID.TYPE_IDS_1_1[0x61], true),
    UINT16(IonTypeID.TYPE_IDS_1_1[0x62], true),
    UINT32(IonTypeID.TYPE_IDS_1_1[0x64], true),
    UINT64(IonTypeID.TYPE_IDS_1_1[0x68], true),
    FLEX_UINT(IonTypeID.TYPE_IDS_1_1[0xF6], true),
    INT8(IonTypeID.TYPE_IDS_1_1[0x61], false),
    INT16(IonTypeID.TYPE_IDS_1_1[0x62], false),
    INT32(IonTypeID.TYPE_IDS_1_1[0x64], false),
    INT64(IonTypeID.TYPE_IDS_1_1[0x68], false),
    FLEX_INT(IonTypeID.TYPE_IDS_1_1[0xF6], false),
    FLOAT16(IonTypeID.TYPE_IDS_1_1[0x6B], false),
    FLOAT32(IonTypeID.TYPE_IDS_1_1[0x6C], false),
    FLOAT64(IonTypeID.TYPE_IDS_1_1[0x6D], false),
    FLEX_STRING(IonTypeID.TYPE_IDS_1_1[0xF9], false),
    FLEX_SYM(IonTypeID.TYPE_IDS_1_1[0xFA], false),
}
