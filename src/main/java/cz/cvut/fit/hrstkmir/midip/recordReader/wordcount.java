package cz.cvut.fit.hrstkmir.midip.recordReader;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcount extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();
		ToolRunner.run(configuration, new wordcount(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception
	{
		Job job = new Job();
		job.setInputFormatClass(CustomFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(wordcount.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(IntSumReducer.class);
		FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(arg0[0]));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(arg0[1]));
		job.submit();
		int rc = (job.waitForCompletion(true) ? 1 : 0);
		return rc;            
	}
}