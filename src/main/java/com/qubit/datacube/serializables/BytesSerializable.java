/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.serializables;

import com.qubit.datacube.CSerializable;

public class BytesSerializable implements CSerializable {
    private final byte[] bytes;
    
    public BytesSerializable(byte[] bytes) {
        this.bytes = bytes;
    }
    
    @Override
    public byte[] serialize() {
        return bytes;
    }
}
