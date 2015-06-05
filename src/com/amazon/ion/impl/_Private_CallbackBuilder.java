// Copyright (c) 2013-2015 Amazon.com, Inc. All rights reserved.

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
