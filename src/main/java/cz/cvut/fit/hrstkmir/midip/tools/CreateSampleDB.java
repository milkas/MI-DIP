
package cz.cvut.fit.hrstkmir.midip.tools;


import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateSampleDB {

    public static void main(String[] args) throws Exception {
        
        String [] pages = {"/", "/a.html", "/b.html", "/c.html"};
        
        HBaseConfiguration hbaseConfig = new HBaseConfiguration();
        HTable htable = new HTable(hbaseConfig, "access_logs2");
        htable.setAutoFlush(false);
        htable.setWriteBufferSize(1024 * 1024 * 12);
        
        int totalRecords = 1000000;
        int maxID = totalRecords / 10000;
        Random rand = new Random();
        System.out.println("importing " + totalRecords + " records ....");
        for (int i=0; i < totalRecords; i++)
        {
            int userID = rand.nextInt(maxID) + 1;
            String randomPage = pages[rand.nextInt(pages.length)];
            Put put = new Put(Bytes.toBytes(i));
            put.add(Bytes.toBytes("details"), Bytes.toBytes("page"), Bytes.toBytes(randomPage));
            put.add(Bytes.toBytes("details"), Bytes.toBytes("user"), Bytes.toBytes(userID));
            htable.put(put);
        }
        htable.flushCommits();
        htable.close();
        System.out.println("done");
    }
}
