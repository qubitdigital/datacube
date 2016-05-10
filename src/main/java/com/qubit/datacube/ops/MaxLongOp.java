package com.qubit.datacube.ops;

import com.qubit.datacube.Deserializer;
import com.qubit.datacube.Op;
import com.qubit.datacube.Util;

public class MaxLongOp implements Op {

    private final long value;

    public MaxLongOp(long value) {
        this.value = value;
    }
    public static final MaxLongOpDeserializer DESERIALIZER = new MaxLongOpDeserializer();

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof MaxLongOp)) {
            throw new RuntimeException();
        }

        return new MaxLongOp(Math.max(value, ((MaxLongOp) otherOp).value));
    }

    @Override
    public Op subtract(Op otherOp) {
        return add(otherOp);
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(value);
    }

    public static class MaxLongOpDeserializer implements Deserializer<MaxLongOp> {
        @Override
        public MaxLongOp fromBytes(byte[] bytes) {
            return new MaxLongOp(Util.bytesToLong(bytes));
        }
    }
}
