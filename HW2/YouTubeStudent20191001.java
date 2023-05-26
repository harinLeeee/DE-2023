import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;


public class YouTubeStudent20191001 {

	public static class Youtube {
		public String category;
		public double rating;
			
		public Youtube(String category, double rating) {
			this.category = category;
			this.rating = rating;
		}
	}	
	
	public static class RatingComparator implements Comparator<Youtube> {
		public int compare(Youtube x, Youtube y) {
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertYoutube(PriorityQueue q, String category, double rating, int topK) {
		Youtube youtube_head = (Youtube)q.peek();
		if (q.size() < topK || youtube_head.rating < rating) {
			Youtube youtube = new Youtube(category, rating);
			q.add(youtube);
			if (q.size() > topK) q.remove();
		}
	}

	public static class YoutubeMapper extends Mapper<Object, Text, Text, Text> {
		Text output_key = new Text();
		Text output_value = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{			
			String category = "";
			String rating = "";
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
				while (itr.hasMoreTokens()) {
					String a = itr.nextToken();
					String b = itr.nextToken();
					String c = itr.nextToken();
					category = itr.nextToken();
					String e = itr.nextToken();
					String f = itr.nextToken();
					rating = itr.nextToken();
				}
			output_key.set(category);
			output_value.set(rating);
			context.write(output_key, output_value);
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		Text reduce_key = new Text();
		DoubleWritable reduce_value = new DoubleWritable();
		
		private PriorityQueue<Youtube> queue;
		private Comparator<Youtube> comp = new RatingComparator();
		private int topK;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, NumberFormatException
		{
			double sum = 0.0;
			double avg = 0.0;
			int count = 0;
			for (Text val : values) {
				try {
					sum += Double.parseDouble(val.toString());
					count++;
				} catch (NumberFormatException e) {
					continue;
				}	
			}
			avg = sum / count;
			insertYoutube(queue, key.toString(), avg, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Youtube>(topK, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (queue.size() != 0) {
				Youtube youtube = (Youtube)queue.remove();
				reduce_key.set(youtube.category);
				reduce_value.set(youtube.rating);
				context.write(reduce_key, reduce_value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: Youtube <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		Job job = new Job(conf, "Youtube");
		job.setJarByClass(YouTubeStudent20191001.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
