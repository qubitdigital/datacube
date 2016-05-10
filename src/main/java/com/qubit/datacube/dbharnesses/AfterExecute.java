package com.qubit.datacube.dbharnesses;

import com.qubit.datacube.Op;

public interface AfterExecute<T extends Op> {
    public void afterExecute(Throwable t);
}