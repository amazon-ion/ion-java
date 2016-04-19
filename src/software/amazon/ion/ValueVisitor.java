/*
 * Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.
 */

package software.amazon.ion;

import software.amazon.ion.util.AbstractValueVisitor;

/**
 * A Visitor for the Ion value hierarchy.
 *
 * @see AbstractValueVisitor
 */
public interface ValueVisitor
{
    public void visit(IonBlob value) throws Exception;

    public void visit(IonBool value) throws Exception;

    public void visit(IonClob value) throws Exception;

    public void visit(IonDatagram value) throws Exception;

    public void visit(IonDecimal value) throws Exception;

    public void visit(IonFloat value) throws Exception;

    public void visit(IonInt value) throws Exception;

    public void visit(IonList value) throws Exception;

    public void visit(IonNull value) throws Exception;

    public void visit(IonSexp value) throws Exception;

    public void visit(IonString value) throws Exception;

    public void visit(IonStruct value) throws Exception;

    public void visit(IonSymbol value) throws Exception;

    public void visit(IonTimestamp value) throws Exception;
}