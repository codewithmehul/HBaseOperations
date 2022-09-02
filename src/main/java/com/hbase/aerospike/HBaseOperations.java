package com.hbase.aerospike;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseOperations {
    public void connect(String[] args) throws IOException {
        Options options = new Options();

        Option nm = new Option("n", "namespace", true, "namespace");
        nm.setRequired(true);
        options.addOption(nm);

        Option tbl = new Option("t", "table", true, "table name");
        tbl.setRequired(true);
        options.addOption(tbl);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }

        String namespaceName = cmd.getOptionValue("namespace");
        String tableName = cmd.getOptionValue("table");

        System.out.println(namespaceName);
        System.out.println(tableName);

        // Create connection and admin client
        Configuration config = HBaseConfiguration.create();

        String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
        config.addResource(new Path(path));

        try {
            HBaseAdmin.available(config);
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running." + e.getMessage());
            return;
        }

        HBaseReader hBaseReader = new HBaseReader();
        hBaseReader.performTableRead(config, namespaceName, tableName);
    }

}
