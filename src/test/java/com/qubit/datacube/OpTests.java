package com.qubit.datacube;

import com.qubit.datacube.ops.DoubleOp;
import com.qubit.datacube.ops.StringOp;
import com.qubit.datacube.ops.MaxLongOp;
import com.qubit.datacube.ops.MinLongOp;
import org.junit.Assert;
import org.junit.Test;

public class OpTests {

    @Test
    public void testDoubleOp() {
        DoubleOp onePt2 = new DoubleOp(1.2);
        byte[] bytes = onePt2.serialize();
        DoubleOp.DoubleOpDeserializer deserializer = new DoubleOp.DoubleOpDeserializer();
        DoubleOp deserialized = deserializer.fromBytes(bytes);
        Assert.assertEquals(onePt2, deserialized);

        Op add = onePt2.add(deserialized);
        Assert.assertEquals(new DoubleOp(2.4), add);
    }

    @Test
    public void testStringOp() {
        StringOp strOp = new StringOp("foo");
        byte[] strBytes = strOp.serialize();
        StringOp.StringOpDeserializer deserializer1 = new StringOp.StringOpDeserializer();
        StringOp deserializedStr = deserializer1.fromBytes(strBytes);
        Assert.assertEquals(strOp, deserializedStr);

        strOp.add(new StringOp("bar")); // should overwrite the old value
        Assert.assertEquals(new StringOp("bar"), strOp);
    }

    @Test
    public void testMinOp() {
        MaxLongOp maxOp = new MaxLongOp(15L);
        byte[] maxBytes = maxOp.serialize();
        MaxLongOp.MaxLongOpDeserializer deserializer1 = new MaxLongOp.MaxLongOpDeserializer();
        MaxLongOp deserializedStr = deserializer1.fromBytes(maxBytes);
        Assert.assertEquals(maxOp, deserializedStr);

        maxOp.add(new MaxLongOp(5L));
        Assert.assertEquals(new MaxLongOp(15L), maxOp);
    }

    @Test
    public void testMaxOp() {
        MinLongOp minOp = new MinLongOp(10L);
        byte[] minBytes = minOp.serialize();
        MinLongOp.MinLongOpDeserializer deserializer1 = new MinLongOp.MinLongOpDeserializer();
        MinLongOp deserializedStr = deserializer1.fromBytes(minBytes);
        Assert.assertEquals(minOp, deserializedStr);

        minOp.add(new MinLongOp(2L));
        Assert.assertEquals(new MinLongOp(2L), minOp);

    }
}
