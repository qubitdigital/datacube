/*
Copyright 2012 Urban Airship and Contributors
*/

package com.qubit.datacube;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.Test;

import com.qubit.datacube.dbharnesses.MapDbHarness;
import com.qubit.datacube.idservices.CachingIdService;
import com.qubit.datacube.idservices.MapIdService;
import com.qubit.datacube.ops.LongOp;


public class SimpleExampleTest {

    /**
     * Test the core datacube logic. Don't use a database, use our in-memory pretend storage
     * backend.
     */
    @Test
    public void writeAndRead() throws Exception {
        IdService idService = new CachingIdService(5, new MapIdService(), "test");
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = 
                new ConcurrentHashMap<BoxedByteArray,byte[]>();
        
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, 
                DbHarness.CommitType.READ_COMBINE_CAS, idService);
        
        DbHarnessTests.basicTest(dbHarness);
    }
}
