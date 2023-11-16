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

import com.amazon.ion.util._Private_FastAppendable;
import java.io.OutputStream;


/**
 * <b>NOT FOR APPLICATION USE!</b>
 */
public final class _Private_FastAppendableTrampoline
{

    public static _Private_FastAppendable forAppendable(Appendable appendable)
    {
        return new AppendableFastAppendable(appendable);
    }

    public static _Private_FastAppendable forOutputStream(
            OutputStream outputStream)
    {
        return new OutputStreamFastAppendable(outputStream);
    }
}
