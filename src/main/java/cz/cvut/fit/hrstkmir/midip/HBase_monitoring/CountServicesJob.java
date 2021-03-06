package cz.cvut.fit.hrstkmir.midip.HBase_monitoring;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class CountServicesJob {

    static class MapperCountCountry extends TableMapper<ImmutableBytesWritable, IntWritable> {

        private int numRecords = 0;
        private static final IntWritable one = new IntWritable(1);

        @Override
        public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {

            int numberOfServices = 10;
            for (int i = 0; i < numberOfServices; i++) {
                byte[] keyByte = values.getValue(Bytes.toBytes("sluzba"), Bytes.toBytes("rok"+(2000+i)));
                    if (keyByte != null){
                    ImmutableBytesWritable key = new ImmutableBytesWritable(keyByte);
                    try {
                        context.write(key, one);

                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                    numRecords++;
                    }
                
            }
        }
    }

    public static class ReducerCountCountry extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {

        FileWriter fw;
        BufferedWriter bw;

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            //Put put = new Put(key.get());
            //put.add(Bytes.toBytes("country"), Bytes.toBytes("inhabitants"), Bytes.toBytes(sum));
            String s = new String(key.get());
            System.out.println("stats :   key :" + s + "  count : " + sum + ".");
            //context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {
        String DB = "lide-sluzby2";
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "Hbase_country_count");
        job.setJarByClass(CountCountryJob.class);
        job.setInputFormatClass(ServicesTableInputFormatBase.class);

        Scan scan = new Scan();
        String columnsString = "osoba";
        byte[] columns = columnsString.getBytes();
        scan.addFamily(columns);
        columnsString = "sluzba";
        columns = columnsString.getBytes();
        scan.addFamily(columns);

        System.out.print(" *******************************" + job.getInputFormatClass().toString());
        TableMapReduceUtil.initTableMapperJob(DB, scan, MapperCountCountry.class, ImmutableBytesWritable.class,
                IntWritable.class, job, false, ServicesTableInputFormatBase.class);
        TableMapReduceUtil.initTableReducerJob("country-counter", ReducerCountCountry.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
