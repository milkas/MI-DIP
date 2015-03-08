package cz.cvut.fit.hrstkmir.midip.HBase_monitoring;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

/**
 * counts the number of userIDs
 *
 * @author sujee ==at== sujee.net
 *
 */
public class FreqCounter1 {

    static class Mapper1 extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private int numRecords = 0;
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
            // extract userKey from the compositeKey (userId + counter)
            ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
            try {
                context.write(userKey, one);

            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            numRecords++;
            if ((numRecords % 10000) == 0) {
                //context.setStatus("mapper processed " + numRecords + " records so far");
            }
        }
    }

    public static class Reducer1 extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        FileWriter fw;
	BufferedWriter bw;
        
        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(key.get());
            put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            System.out.println(String.format("stats :   key : %d,  count : %d", Bytes.toInt(key.get()), sum));
            context.write(key, put);
            
//            			for(Map.Entry<String, List<Long>> entry : map.entrySet())
//			{       
//                                //improvizace pro pripad ze v textu je strednik, ktery by pak rozhodil parsovani
//                                String key2 = entry.getKey();
//                                if(key2.contains(";"))
//                                    key2.replace(";", "semicolon");
//                                    
//				bw.write(entry.getKey()+";");  //lip se to parsuje...
//				tmpList = (List<Long>) entry.getValue();
//				bw.write(tmpList.size()+";");
//				
//				Iterator<Long> iterator = tmpList.iterator();
//				while (iterator.hasNext())
//				{
//					bw.write(iterator.next()+" ");
//				}
//				bw.newLine();
//			}
//			
//			bw.close();
//			fw.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "Hbase_FreqCounter1");
        job.setJarByClass(FreqCounter1.class);
        job.setInputFormatClass(MyTableInputFormatBase.class);
        
        Scan scan = new Scan();
        String columnsString = "details"; // comma seperated
        byte[] columns = columnsString.getBytes();
        scan.addFamily(columns);
        scan.setFilter(new FirstKeyOnlyFilter());
        System.out.print(" *******************************" + job.getInputFormatClass().toString());
        TableMapReduceUtil.initTableMapperJob("access_logs", scan, Mapper1.class, ImmutableBytesWritable.class,
                IntWritable.class, job, false, MyTableInputFormatBase.class);
        TableMapReduceUtil.initTableReducerJob("summary_user", Reducer1.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
