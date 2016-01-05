// Copyright (c) 2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;


/**
 * Common options for writing Ion data streams of any form.
 * <p>
 * <b>WARNING:</b> This class should not be extended by code outside of
 * this library.
 * <p>
 *
 * @since IonJava R15
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
         *
         * @since IonJava R16
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
     *
     * @since IonJava R16
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
     *
     * @since IonJava R16
     */
    public abstract IvmMinimizing getIvmMinimizing();
}
