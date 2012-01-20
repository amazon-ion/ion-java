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
        // ENSURE,

        /**
         * Indicates that initial IVMs should be suppressed from the output
         * stream whenever possible, even when they are explicitly written.
         */
        SUPPRESS
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
}
