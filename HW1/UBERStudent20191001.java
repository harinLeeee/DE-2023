import java.io.IOException;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.TextStyle;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20191001 {
	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text key1 = new Text();
		private Text key2 = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] values = value.toString().split(",");
			String baseNum = values[0];
			String date = values[1];
			String vehicles = values[2];
			String trips = values[3];
			
			String[] d = date.split("/");
			int year = Integer.parseInt(d[2]);
			int month = Integer.parseInt(d[0]);
			int day = Integer.parseInt(d[1]);
			LocalDate ld = LocalDate.of(year, month, day);
			DayOfWeek dow = ld.getDayOfWeek();
			date = dow.getDisplayName(TextStyle.SHORT, Locale.US).toUpperCase();
			
			key1.set(baseNum + "," + date);
			key2.set(trips + "," + vehicles);
			context.write(key1, key2);
		}
	}
	
	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		private Text output = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int vSum = 0;
			int tSum = 0;
			for (Text val : values) {
				String[] results = val.toString().split(",");
				tSum += Integer.parseInt(results[0]);
				vSum += Integer.parseInt(results[1]);
			}
			output.set(String.valueOf(tSum) + "," + String.valueOf(vSum));
			context.write(key, output);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20191001.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
