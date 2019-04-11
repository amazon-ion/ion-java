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

package com.amazon.ion;

/**
 * Indicates the smallest-possible Java type of an Ion {@code int} value.
 */
public enum IntegerSize
{
    /**
     * Fits in the Java {@code int} primitive (four bytes).
     * The value can be retrieved through methods like {@link IonReader#intValue()}
     * or {@link IonInt#intValue()} without data loss.
     */
    INT,

    /**
     * Fits in the Java {@code int} primitive (eight bytes).
     * The value can be retrieved through methods like {@link IonReader#longValue()}
     * or {@link IonInt#longValue()} without data loss.
     */
    LONG,

    /**
     * Larger than eight bytes. This value can be retrieved through methods like
     * {@link IonReader#bigIntegerValue()} or {@link IonInt#bigIntegerValue()}
     * without data loss.
     */
    BIG_INTEGER,

}
