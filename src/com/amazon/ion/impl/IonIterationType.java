// Copyright (c) 2010-2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonException;

/**
 *
 */
public enum IonIterationType
{
    SYSTEM_TEXT,
    SYSTEM_BINARY,
    SYSTEM_ION_VALUE,
    USER_TEXT,
    USER_BINARY,
    USER_ION_VALUE;

    public boolean isText() {
        switch (this) {
            case SYSTEM_TEXT:
            case USER_TEXT:
                return true;
            case SYSTEM_BINARY:
            case USER_BINARY:
                return false;
            case SYSTEM_ION_VALUE:
            case USER_ION_VALUE:
                return false;
        }
        throw new IonException("unrecognized writer type enountered: "+this.toString());
    }
    public boolean isBinary() {
        switch (this) {
        case SYSTEM_TEXT:
        case USER_TEXT:
            return false;
        case SYSTEM_BINARY:
        case USER_BINARY:
            return true;
        case SYSTEM_ION_VALUE:
        case USER_ION_VALUE:
            return false;
        }
        throw new IonException("unrecognized writer type enountered: "+this.toString());
    }
    public boolean isIonValue() {
        switch (this) {
            case SYSTEM_TEXT:
            case USER_TEXT:
                return false;
            case SYSTEM_BINARY:
            case USER_BINARY:
                return false;
            case SYSTEM_ION_VALUE:
            case USER_ION_VALUE:
                return true;
        }
        throw new IonException("unrecognized writer type enountered: "+this.toString());
    }
    public boolean isUser() {
        switch (this) {
        case SYSTEM_TEXT:
        case SYSTEM_BINARY:
        case SYSTEM_ION_VALUE:
            return false;
        case USER_TEXT:
        case USER_BINARY:
        case USER_ION_VALUE:
            return true;
        }
        throw new IonException("unrecognized writer type enountered: "+this.toString());
    }
    public boolean isSystem() {
        switch (this) {
        case SYSTEM_TEXT:
        case SYSTEM_BINARY:
        case SYSTEM_ION_VALUE:
            return true;
        case USER_TEXT:
        case USER_BINARY:
        case USER_ION_VALUE:
            return false;
        }
        throw new IonException("unrecognized writer type enountered: "+this.toString());
    }
}
