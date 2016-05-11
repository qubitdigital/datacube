/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.bucketers;


import com.qubit.datacube.CSerializable;
import com.qubit.datacube.serializables.StringSerializable;

/**
 * You can use this bucketer to avoid writing your own, in the case where:
 *  - You have a cube coordinate that's a String
 *  - You want the bucketer to pass through the String unchanged as the bucket
 */
public class StringToBytesBucketer extends AbstractIdentityBucketer<String> {
    private static final StringToBytesBucketer instance = new StringToBytesBucketer();
    
    public static final StringToBytesBucketer getInstance() {
        return instance;
    }

    @Override
    public CSerializable makeSerializable(String coord) {
        return new StringSerializable(coord);
    }
}
