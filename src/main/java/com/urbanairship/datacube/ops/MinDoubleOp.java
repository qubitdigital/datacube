package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

public class MinDoubleOp implements Op {
    private final double val;

    public MinDoubleOp(double value) {
        this.val = value;
    }
    public static final MinDoubleOpDeserializer DESERIALIZER = new MinDoubleOpDeserializer();

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof MinDoubleOp)) {
            throw new RuntimeException();
        }

        return new MinDoubleOp(Math.min(val, ((MinDoubleOp) otherOp).val));
    }

    @Override
    public Op subtract(Op otherOp) {
        return add(otherOp);
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(Double.doubleToRawLongBits(val));
    }

    public static class MinDoubleOpDeserializer implements Deserializer<MinDoubleOp> {
        @Override
        public MinDoubleOp fromBytes(byte[] bytes) {
            return new MinDoubleOp(Double.longBitsToDouble(Util.bytesToLong(bytes)));
        }
    }
}
