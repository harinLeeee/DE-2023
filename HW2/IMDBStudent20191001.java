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


public class IMDBStudent20191001 {

	public static class Movie {
		public String title;
		public double rating;
			
		public Movie(String title, double rating) {
			this.title = title;
			this.rating = rating;
		}
	}	
	
	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	
	public static void insertMovie(PriorityQueue q, String title, double rating, int topK) {
		Movie movie_head = (Movie)q.peek();
		if (q.size() < topK || movie_head.rating < rating) {
			Movie movie = new Movie(title, rating);
			q.add(movie);
			if (q.size() > topK) q.remove();
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, Text, Text> {
		boolean fileM = true;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Text outputKey = new Text();
			Text outputValue = new Text();
			String joinKey = "";	// movie id
			String o_value = "";
			String title = "";
			String genre = "";
			String[] values = value.toString().split("::");
			if (fileM) {
				joinKey = values[0];
				title = values[1];
				genre = values[2];
				
				StringTokenizer itr = new StringTokenizer(genre, "|");
				while (itr.hasMoreTokens()) {
					if (itr.nextToken().equals("Fantasy")) {
						o_value = title;
						outputKey.set(joinKey);
						outputValue.set(o_value);
						context.write(outputKey, outputValue);
					}
				}
			}
			else {
				joinKey = values[1];
				String rating = values[2];
				o_value = rating;
				outputKey.set(joinKey);
				outputValue.set(o_value);
				context.write(outputKey, outputValue);
			}
		}
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			if (filename.indexOf("movies.dat") != -1) fileM = true;
			else fileM = false;
		}
	}
	
	public static class IMDBReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		Text reduce_key = new Text();
		DoubleWritable reduce_value = new DoubleWritable();
		
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, NumberFormatException
		{
			int sum = 0;
			double avg = 0.0;
			int count = 0;
			boolean isFantasy = false;
			String title = "";
			for (Text val : values) {
				try {
					sum += Integer.parseInt(val.toString());
					count++;
				} catch (NumberFormatException e) { // title
					//reduce_key.set(val);
					title = val.toString();
					isFantasy = true;
				}
			}
			if (isFantasy) {
				avg = (double)sum / count;
				//reduce_value.set(avg);
				//context.write(reduce_key, reduce_value);
				insertMovie(queue, title, avg, topK);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while (queue.size() != 0) {
				Movie movie = (Movie)queue.remove();
				reduce_key.set(movie.title);
				reduce_value.set(movie.rating);
				context.write(reduce_key, reduce_value);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20191001.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
