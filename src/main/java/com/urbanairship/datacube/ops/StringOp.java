package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;

/**
 * Created by qasim on 13/04/2016.
 */
public class StringOp implements Op {
    private final String val;
    public static final StringOpDeserializer DESERIALIZER = new StringOpDeserializer();

    public StringOp(String val) {
        this.val = val;
    }

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof StringOp)) {
            throw new RuntimeException();
        }
        return new StringOp(((StringOp) otherOp).val);
    }

    @Override
    public Op subtract(Op otherOp) {
            return add(otherOp);
    }

    @Override
    public int hashCode() {
        long temp = val.equals("") ? 0L: Long.parseLong(val);
        return (int) (temp ^ (temp >>> 32));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StringOp other = (StringOp) obj;

        return val.equals(other.val);
    }

    @Override
    public byte[] serialize() {
        return val.getBytes();
    }

    public static class StringOpDeserializer implements Deserializer<StringOp> {
        @Override
        public StringOp fromBytes(byte[] bytes) {
            return new StringOp(new String(bytes));
        }
    }

    public String toString() {
        return val;
    }

    public String getValue() {
        return val;
    }
}
