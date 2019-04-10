/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.ion.impl;

import static org.junit.Assert.*;

import org.junit.Assert;
import org.junit.Test;
import com.amazon.ion.Decimal;
import static org.junit.Assert.*;

public class _Private_ScalarConversionsTest {
    private long decimalToLong(final Decimal d) {
        _Private_ScalarConversions.ValueVariant v = new _Private_ScalarConversions.ValueVariant();
        v.setValue(d);
        v.cast(_Private_ScalarConversions.FNID_FROM_DECIMAL_TO_LONG);
        return v.getLong();
    }
    @Test
    public void decimalToLong() {
        assertEquals(1, decimalToLong(Decimal.valueOf(1L)));
    }
    @Test
    public void decimalToMinLong() {
        assertEquals(Long.MAX_VALUE, decimalToLong(Decimal.valueOf(Long.MAX_VALUE)));
    }
    @Test
    public void decimalToMaxLong() {
        assertEquals(Long.MIN_VALUE, decimalToLong(Decimal.valueOf(Long.MIN_VALUE)));
    }
}
