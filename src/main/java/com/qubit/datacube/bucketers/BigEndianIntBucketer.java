/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.qubit.datacube.BucketType;
import com.qubit.datacube.CSerializable;
import com.qubit.datacube.serializables.IntSerializable;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Integer.
 */
public class BigEndianIntBucketer extends AbstractIdentityBucketer<Integer> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);

    @Override
    public CSerializable makeSerializable(Integer coord) {
        return new IntSerializable(coord);
    }
}
