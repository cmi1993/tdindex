package cn.edu.scnu.dtindex.tools;

import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountMapper;
import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountReducer;
import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountRunner;
import cn.edu.scnu.dtindex.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CalcScale {
	static long x_max = Long.MIN_VALUE;
	static long x_min = Long.MAX_VALUE;
	static long y_max = Long.MIN_VALUE;
	static long y_min = Long.MAX_VALUE;

	static class CalcScaleMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
			context.write(new IntWritable(1), t);
		}
	}

	static class CalcScaleReducer extends Reducer<IntWritable, Tuple, Text, NullWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
			for (Tuple tuple : values) {
				if (x_max < tuple.getVt().getStart()) x_max = tuple.getVt().getStart();
				if (x_min > tuple.getVt().getStart()) x_min = tuple.getVt().getStart();
				if (y_max < tuple.getVt().getEnd()) y_max = tuple.getVt().getEnd();
				if (y_min > tuple.getVt().getEnd()) y_min = tuple.getVt().getEnd();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text(x_max + " " + x_min + " " + y_max + " " + y_min), NullWritable.get());
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.job.jar", "/Users/think/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Job job = Job.getInstance(conf, "calc");
		job.setJarByClass(CalcScale.class);
		job.setMapperClass(CalcScaleMapper.class);
		job.setReducerClass(CalcScaleReducer.class);
		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Tuple.class);
		// 【设置我们的业务逻辑Reducer类输出的key和value的数据类型】
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(9);
		FileInputFormat.setInputPaths(job, "hdfs://master:8020/timeData/1000w/classifiedData/");
		Path outPath = new Path("hdfs://master:8020/test/count_out");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// 向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);

	}
}
