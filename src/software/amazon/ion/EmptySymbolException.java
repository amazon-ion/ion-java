/*
 * Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion;

/**
 * An error caused by a symbol not containing at least one character for
 * its text.
 *
 * @deprecated this exception is not used as empty symbols are valid. In cases where null is used as the symbol value it was
 * replaced by {@link NullPointerException}
 */
@Deprecated
public class EmptySymbolException
    extends IonException
{
    private static final long serialVersionUID = -7801632953459636349L;

    public EmptySymbolException()
    {
        super("Symbols must contain at least one character.");
    }
}
