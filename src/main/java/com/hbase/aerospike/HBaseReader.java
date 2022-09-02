package com.hbase.aerospike;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class HBaseReader {
    public void performTableRead(Configuration config, String namespaceName, String tableName) {

        try {
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            TableName tbl = TableName.valueOf(tableName);

            if (admin.tableExists(tbl)) {
                Table table = connection.getTable(tbl);
                scanTable(table);
            }
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private void scanTable(Table table) throws IOException {
        Scan scan = new Scan();
        try {
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner)
                System.out.println("Found now: " + result);
        }
        catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }
}


