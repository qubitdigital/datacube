/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.bucketers;

import com.qubit.datacube.CSerializable;
import com.qubit.datacube.Util;
import com.qubit.datacube.serializables.BytesSerializable;

public class EnumToOrdinalBucketer<T extends Enum<?>> extends AbstractIdentityBucketer<T> {
    private final int numBytes;
    
    public EnumToOrdinalBucketer(int numBytes) {
        this.numBytes = numBytes;
    }
    
    @Override
    public CSerializable makeSerializable(T coordinate) {
        int ordinal = coordinate.ordinal();
        byte[] bytes = Util.trailingBytes(Util.intToBytes(ordinal), numBytes);
        
        return new BytesSerializable(bytes);
    }
}
