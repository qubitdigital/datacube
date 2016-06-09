/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube.collectioninputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.qubit.datacube.backfill.CollectionWritable;

/**
 * A Hadoop InputFormat that lets you use an arbitrary Collection as input to a mapreduce job.
 * Each item in the input Collection will be given to a single map() call as its key. 
 */
public class CollectionInputFormat extends InputFormat<Writable,NullWritable> {
    public static final String CONFKEY_COLLECTION = "collectioninputformat.collectionitems";
    public static final String CONFKEY_VALCLASS = "collectioninputformat.valueclass";
    
    /**
     * Stores the given collection as a configuration value. When getSplits() runs later,
     * it will be able to deserialize the collection and use it.
     */
    public static <T extends Writable> void setCollection(Job job, Collection<Scan> scans) throws IOException {
        job.getConfiguration().set(CONFKEY_COLLECTION, toBase64(scans));
        job.getConfiguration().set(CONFKEY_VALCLASS, "Scan");
    }

    public static String toBase64(Collection<Scan> scans) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        out.writeUTF("Scan");
        out.writeInt(scans.size());
        for (Scan s : scans) {
            byte[] b = ProtobufUtil.toScan(s).toByteArray();
            out.writeInt(b.length);
            out.write(b);
        }

        byte[] rawBytes = Arrays.copyOf(out.getData(), out.getLength());
        return new String(Base64.encodeBase64(rawBytes));
    }
    
    @SuppressWarnings("unchecked")
    public static Collection<Scan> fromBase64(String base64) throws IOException {
        Collection<Scan> ret = new ArrayList<>();
        byte[] rawBytes = Base64.decodeBase64(base64.getBytes());
        DataInputBuffer in = new DataInputBuffer();
        in.reset(rawBytes, rawBytes.length);
        String name = in.readUTF();
        if(name.equals("Scan")) {
            int numElements = in.readInt();
            for(int i=0; i<numElements; i++) {
                byte[] b = new byte[in.readInt()];
                in.readFully(b);
                Scan s = ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(b));
                ret.add(s);
            }
        }else{
            throw new RuntimeException("Error");
        }

        return ret;
    }
    
    @Override
    public RecordReader<Writable, NullWritable> createRecordReader(final InputSplit split, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        return new SingleItemRecordReader((SingleValueSplit)split);

    }

    @SuppressWarnings("unchecked")
    @Override
    public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
        String base64 = ctx.getConfiguration().get(CONFKEY_COLLECTION);
        String valueClassName = ctx.getConfiguration().get(CONFKEY_VALCLASS);
        /*
        Class<? extends Writable> valueClass;
        try {
            valueClass = (Class<? extends Writable>)Class.forName(valueClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }*/

        Collection<Scan> collection = fromBase64(base64);
        List<InputSplit> splits = new ArrayList<InputSplit>(collection.size());
        for(Scan val: collection) {
            splits.add(new SingleValueSplit());
        }
        return splits;
    }
}
