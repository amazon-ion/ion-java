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

/**
 * NOT FOR APPLICATION USE!
 * <p>
 * Interface used by {@link _Private_IonTextWriterBuilder} to allow
 * instantiation of a new {@link _Private_MarkupCallback} during every
 * IonTextWriterBuilder.build;
 */
public interface _Private_CallbackBuilder
{
    _Private_MarkupCallback build(_Private_FastAppendable rawOutput);
}
