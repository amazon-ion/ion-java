/*
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.system;

import java.io.OutputStream;
import software.amazon.ion.IonWriter;

/**
 * Common options for writing Ion data streams of any form.
 * <p>
 * <b>WARNING:</b> This class should not be extended by code outside of
 * this library.
 */
public abstract class IonWriterBuilder
{
    /**
     * A strategy for altering emission of Ion version markers at the start of
     * an Ion stream.
     *
     * @see IonTextWriterBuilder#setInitialIvmHandling(IonWriterBuilder.InitialIvmHandling)
     */
    public enum InitialIvmHandling
    {
        /**
         * Always emits an initial IVM, even when the user hasn't explicitly
         * written one.  If the user <em>does</em> write one, this won't
         * cause an extra to be emitted.
         */
        ENSURE,

        /**
         * Indicates that initial IVMs should be suppressed from the output
         * stream whenever possible, even when they are explicitly written.
         */
        SUPPRESS
    }


    /**
     * A strategy for minimizing the output of non-initial Ion version markers.
     * <p>
     * This strategy does not affect handling of IVMs at the start of a data
     * stream; that's the job of {@link InitialIvmHandling}.
     *
     * @see IonTextWriterBuilder#setIvmMinimizing(IonWriterBuilder.IvmMinimizing)
     */
    public enum IvmMinimizing
    {
        /** Replaces identical, adjacent IVMs with a single IVM. */
        ADJACENT,

        /**
         * Discards IVMs that don't change the Ion version, even when there's
         * other data between them. This includes adjacent IVMs.
         */
        DISTANT
    }


    /**
     * NOT FOR APPLICATION USE
     */
    IonWriterBuilder()
    {
    }


    /**
     * Gets the strategy for emitting Ion version markers at the start
     * of the stream. By default, IVMs are emitted only when explicitly
     * written or when necessary (for example, before data that's not Ion 1.0,
     * or at the start of Ion binary output).
     *
     * @return the initial IVM strategy.
     * Null indicates the default for the specific output form.
     */
    public abstract InitialIvmHandling getInitialIvmHandling();


    /**
     * Gets the strategy for eliminating Ion version markers mid-stream.
     * By default, IVMs are emitted as received or when necessary.
     * <p>
     * This strategy does not affect handling of IVMs at the start of the
     * stream; that's the job of {@link InitialIvmHandling}.
     *
     * @return the IVM minimizing strategy.
     */
    public abstract IvmMinimizing getIvmMinimizing();

    /**
     * Builds a new writer based on this builder's configuration
     * properties.
     *
     * @param out the stream that will receive Ion data.
     * Must not be null.
     *
     * @return a new {@link IonWriter} instance; not {@code null}.
     */
    public abstract IonWriter build(OutputStream out);
}
