/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.qubit.datacube.CSerializable;
import com.qubit.datacube.serializables.LongSerializable;
import com.qubit.datacube.BucketType;
import com.qubit.datacube.Bucketer;
import com.qubit.datacube.ops.LongOp;

/**
 * You can use this when one of your dimension coordinate types is a straightforward Long.
 */
public class BigEndianLongBucketer implements Bucketer<Long> {
    private final static List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    
    @Override
    public SetMultimap<BucketType,CSerializable> bucketForWrite(Long coordinate) {
        return ImmutableSetMultimap.<BucketType,CSerializable>of(
                BucketType.IDENTITY, new LongOp(coordinate));
    }

    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        assert bucketType == BucketType.IDENTITY;
        return new LongSerializable((Long)coordinate);
    }

    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }
}
