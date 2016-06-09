/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.collectioninputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * For internal use by CollectionInputFormat.
 */
public class SingleValueSplit extends InputSplit implements Writable {

    public SingleValueSplit() { } // No-op constructor needed for deserialization
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(0);
    }
    
    public Writable getKey() {
        return null;
    }
}