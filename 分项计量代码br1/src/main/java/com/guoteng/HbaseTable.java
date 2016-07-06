package com.guoteng;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseTable {
	static Configuration cfg=HBaseConfiguration.create();
    //创建一张表，通过HBaseAdmin HTableDescriptor来创建
    public static void creat(String tablename,List<String> columnFamilys) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(cfg);
        if(!admin.tableExists(tablename)){
        	HTableDescriptor tableDesc = new HTableDescriptor(tablename);
            for (String columnFamily : columnFamilys) {
    			HColumnDescriptor hColDesc=new HColumnDescriptor(//
    					Bytes.toBytes(columnFamily), 0,10, 
    					KeepDeletedCells.FALSE,
    					new String(Bytes.toBytes("NONE")),
    					true,new String(Bytes.toBytes("NONE")),
    					true, false, 64 * 1024 , Integer.MAX_VALUE,
    					new String(Bytes.toBytes("ROW")), 0);
    			tableDesc.addFamily(hColDesc);
    			byte[][] regins=new byte[][]{"01".getBytes(),"02".getBytes(),"03".getBytes(),
    					"04".getBytes(),"05".getBytes(),"06".getBytes(),"07".getBytes(),
    					"08".getBytes(),"09".getBytes(),"10".getBytes(),"11".getBytes(),
    					"12".getBytes(),"13".getBytes()};
	            admin.createTable(tableDesc, regins);
                System.out.println("create table success!");
            }
        }
    }

    public static void put(String tablename,String row, String columnFamily,String column,
    		String string) throws Exception {
        HTable table = new HTable(cfg, tablename);
        Put p1=new Put(row.getBytes());
        p1.add(columnFamily.getBytes(), column.getBytes(), String.valueOf(string).getBytes());
        table.put(p1);
        System.out.println("put '"+row+"','"+columnFamily+":"+column+"','"+string+"'");
    }
}
