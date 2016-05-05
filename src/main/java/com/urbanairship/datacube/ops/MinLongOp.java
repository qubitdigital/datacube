package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

public class MinLongOp implements Op {

    private final long value;

    public MinLongOp(long value) {
        this.value = value;
    }
    public static final MinLongOpDeserializer DESERIALIZER = new MinLongOpDeserializer();

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof MinLongOp)) {
            throw new RuntimeException();
        }

        return new MaxLongOp(Math.max(value, ((MinLongOp) otherOp).value));
    }

    @Override
    public Op subtract(Op otherOp) {
        return add(otherOp);
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(value);
    }

    public static class MinLongOpDeserializer implements Deserializer<MinLongOp> {
        @Override
        public MinLongOp fromBytes(byte[] bytes) {
            return new MinLongOp(Util.bytesToLong(bytes));
        }
    }
}
