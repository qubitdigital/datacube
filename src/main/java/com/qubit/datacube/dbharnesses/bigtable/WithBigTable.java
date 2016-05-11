package com.qubit.datacube.dbharnesses.bigtable;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class WithBigTable {
    private static final Logger log = LoggerFactory.getLogger(WithBigTable.class);

    public static <T> T run(Connection connection, byte[] tableName, BigTableRunnable<T> runnable)
            throws IOException {

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            return runnable.runWith(table);
        } catch(Exception e) {
            if(e instanceof IOException) {
                throw (IOException)e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static interface BigTableRunnable<T> {
        public T runWith(Table table) throws IOException;
    }

    /**
     * Do an HBase put and return null.
     * @throws IOException if the underlying HBase operation throws an IOException
     */
    public static void put(Connection connection, byte[] tableName, final Put put) throws IOException {
        run(connection, tableName, new BigTableRunnable<Object>() {
            @Override
            public Object runWith(Table table) throws IOException {
                table.put(put);
                return null;
            }
        });
    }

    public static Result get(Connection connection, byte[] tableName, final Get get) throws IOException {
        return run(connection, tableName, new BigTableRunnable<Result>() {
            @Override
            public Result runWith(Table table) throws IOException {
                return table.get(get);
            }
        });
    }

    public static long increment(Connection connection, byte[] tableName, final byte[] row,
                                 final byte[] cf, final byte[] qual, final long amount) throws IOException {
        return run(connection, tableName, new BigTableRunnable<Long> () {
            @Override
            public Long runWith(Table table) throws IOException {
                return table.incrementColumnValue(row, cf, qual, amount);
            }
        });
    }

    public static boolean checkAndPut(Connection connection, byte[] tableName, final byte[] row,
                                      final byte[] cf, final byte[] qual, final byte[] value, final Put put)
            throws IOException {
        return run(connection, tableName, new BigTableRunnable<Boolean>() {
            @Override
            public Boolean runWith(Table table) throws IOException {
                return table.checkAndPut(row, cf, qual, value, put);
            }
        });
    }

    public static boolean checkAndDelete(Connection connection, byte[] tableName, final byte[] row,
                                         final byte[] cf, final byte[] qual, final byte[] value, final Delete delete)
            throws IOException {
        return run(connection, tableName, new BigTableRunnable<Boolean>() {
            @Override
            public Boolean runWith(Table table) throws IOException {
                return table.checkAndDelete(row, cf, qual, value, delete);
            }
        });
    }

    static class WrappedInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        public final InterruptedException wrappedException;

        public WrappedInterruptedException(InterruptedException ie) {
            this.wrappedException = ie;
        }
    }

    public static Object[] batch(Connection connection, byte[] tableName, final List<Row> actions)
            throws IOException, InterruptedException {
        try {
            return run(connection, tableName, new BigTableRunnable<Object[]>() {
                @Override
                public Object[] runWith(Table table) throws IOException {
                    try {
                        // TODO
                        // Deprecated call
                        Object[] results = new Object[actions.size()];
                        table.batch(actions, results);
                        return results;
                    } catch (InterruptedException e) {
                        throw new WrappedInterruptedException(e);
                    }
                }
            });
        } catch (WrappedInterruptedException e) {
            throw e.wrappedException;
        }
    }

    public static Result[] get(Connection connection, byte[] tableName, final List<Get> gets)
            throws IOException {
        return run(connection, tableName, new BigTableRunnable<Result[]>() {
            @Override
            public Result[] runWith(Table table) throws IOException {
                return table.get(gets);
            }
        });
    }

    public static interface ScanRunnable<T> {
        public T run(ResultScanner rs) throws IOException;
    }

    public static <T> T scan(Connection connection, byte[] tableName, final Scan scan,
                             final ScanRunnable<T> scanRunnable) throws IOException {
        return run(connection, tableName, new BigTableRunnable<T>() {
            @Override
            public T runWith(Table table) throws IOException {
                ResultScanner rs = null;
                try {
                    rs = table.getScanner(scan);
                    return scanRunnable.run(rs);
                } finally {
                    if(rs != null) {
                        rs.close();
                    }
                }
            }
        });
    }
}
