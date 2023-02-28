package Kcore;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KCore {
	 public static class KCoreMapper extends Mapper<Object, Text, Text, IntWritable> {

	        private final static IntWritable one = new IntWritable(1);
	        private final static Text nodeId = new Text();

	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	            String[] nodes = value.toString().split("\t");
	            String node = nodes[0];
	            String[] neighbors = nodes[1].split(",");
	            nodeId.set(node);
	            for (String neighbor : neighbors) {
	                context.write(new Text(neighbor), one);
	            }
	        }
	 }
	 public static class KCoreReducer extends Reducer<Text, IntWritable, Text, Text> {

	        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	            int degree = 0;
	            for (IntWritable val : values) {
	                degree += val.get();
	            }
	            context.write(new Text(Integer.toString(degree)), key);
	        }
	    }

	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "kcore");
	        job.setJarByClass(KCore.class);
	        job.setMapperClass(KCoreMapper.class);
	        job.setReducerClass(KCoreReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
}
