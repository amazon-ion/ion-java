// Copyright (c) 2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.system;


/**
 * Common options for writing Ion data streams of any form.
 * <p>
 * <b>
 * This class is not intended to be used as an application extension point;
 * do not extend or implement it.
 * </b>
 *
 * @since IonJava R15
 */
public abstract class IonWriterBuilder
{
    /**
     * A strategy for altering emission of Ion version markers at the start of
     * an Ion stream.
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
     */
    public enum IvmMinimizing
    {
        /** Replaces identical, adjacent IVMs with a single IVM. */
        ADJACENT,

        /*
         * Discards IVMs that don't change the Ion version, even when there's
         * other data between them.
         */
//        DISTANT
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
     * Gets the strategy for reducing or eliminating non-initial Ion version
     * markers. When null, IVMs are emitted as they are written.
     *
     * @return the initial IVM minimization.
     */
    public abstract IvmMinimizing getIvmMinimizing();
}
