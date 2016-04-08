package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.Util;

public class MaxLongOp implements Op {

    private final long value;

    public MaxLongOp(long value) {
        this.value = value;
    }

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

}
