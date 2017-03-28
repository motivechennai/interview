package com.alcatel.motive.interview;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class MyConfiguration {

    String tableName = "PhoneBook";

	static Connection conn;
    static HBaseAdmin admin;
    
    static{
    
    	Configuration conf = HBaseConfiguration.create();
    try {
		conn = ConnectionFactory.createConnection(conf);
        admin = new HBaseAdmin(conf);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    }

    
    public Table getTable(byte[] colFamily,byte[] qualifier){
        HTableDescriptor tabDesc = new HTableDescriptor(Bytes.toBytes(tableName));
        tabDesc.addFamily(new HColumnDescriptor(colFamily));
        
        try {
			if(!admin.tableExists(tableName)){
			admin.createTable(tabDesc);
			System.out.println("Table created");
			}
			else{
				System.out.println("Table already exists.. Hence, not creating again..");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERROR in creating "+tableName + " : " + e.getMessage());
		}

        Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("ERROR in connecting to "+tableName + " : " + e.getMessage());
		}
        return table;
    }
}


