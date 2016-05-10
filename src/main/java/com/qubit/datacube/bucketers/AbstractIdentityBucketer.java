package com.qubit.datacube.bucketers;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.qubit.datacube.BucketType;
import com.qubit.datacube.CSerializable;
import com.qubit.datacube.Bucketer;

/**
 * An implementation of the {@link cBucketer} to make it easy to create a bucketer that 
 * always uses the identity bucket type ({@link BucketType#IDENTITY}).
 */
public abstract class AbstractIdentityBucketer<T> implements Bucketer<T> {
    private final List<BucketType> bucketTypes = ImmutableList.of(BucketType.IDENTITY);
    
    @Override
    public List<BucketType> getBucketTypes() {
        return bucketTypes;
    }
    
    @Override
    public SetMultimap<BucketType, CSerializable> bucketForWrite(T coordinate) {
        return ImmutableSetMultimap.of(BucketType.IDENTITY, makeSerializable(coordinate));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public CSerializable bucketForRead(Object coordinate, BucketType bucketType) {
        return makeSerializable((T)coordinate);
    }

    public abstract CSerializable makeSerializable(T coord);
}
