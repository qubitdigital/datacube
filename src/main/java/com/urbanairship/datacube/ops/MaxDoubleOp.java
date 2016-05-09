package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

public class MaxDoubleOp implements Op {

    private final double val;

    public MaxDoubleOp(double value) {
        this.val = value;
    }
    public static final MaxDoubleOpDeserializer DESERIALIZER = new MaxDoubleOpDeserializer();

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof MaxDoubleOp)) {
            throw new RuntimeException();
        }

        return new MaxDoubleOp(Math.max(val, ((MaxDoubleOp) otherOp).val));
    }

    @Override
    public Op subtract(Op otherOp) {
        return add(otherOp);
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(Double.doubleToRawLongBits(val));
    }

    public static class MaxDoubleOpDeserializer implements Deserializer<MaxDoubleOp> {
        @Override
        public MaxDoubleOp fromBytes(byte[] bytes) {
            return new MaxDoubleOp(Double.longBitsToDouble(Util.bytesToLong(bytes)));
        }
    }
}
