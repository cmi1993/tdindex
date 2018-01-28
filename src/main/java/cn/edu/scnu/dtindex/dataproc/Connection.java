package cn.edu.scnu.dtindex.dataproc;


import cn.edu.scnu.dtindex.model.Course;
import cn.edu.scnu.dtindex.model.NoTime;
import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.model.ValidTime;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import cn.edu.scnu.dtindex.tools.DFSIOTools;
import cn.edu.scnu.dtindex.tools.HDFSTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 分布式时态数据连接
 */
public class Connection {
	static private CONSTANTS cos = CONSTANTS.getInstance();
	private static Map<Integer, Course> CouseMap = new HashMap<Integer, Course>();

	static class ConnectionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String courseTablePath = cos.getCourseTablePath();
			String result = DFSIOTools.toReadWithCharReturn(context.getConfiguration(), courseTablePath);
			String[] records = result.split("\n");
			for (String record : records) {
				String[] fields = record.split(",");
				Course course = new Course(fields[0], fields[1], fields[2], fields[3]);
				CouseMap.put(course.getCid(), course);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			//fields[3]就是cid
			Course course = CouseMap.get(Integer.parseInt(fields[3]));
			ValidTime[] times = new ValidTime[2];
			times[0]=new ValidTime(fields[4],fields[5]);
			times[1]=new ValidTime(course.getStart_time(),course.getEnd_time());
			ValidTime intersect = ValidTime.intersect(times);
			context.write(new Text(fields[0]+","+fields[1]+","+fields[2]+","+fields[3]+","+course.getTeacherName()+","+course.getStart_time()+","+course.getEnd_time()),NullWritable.get());
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name", "yarn");

		conf.set("mapreduce.job.jar", "/Users/think/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Job job = Job.getInstance(conf, "distributionProject_cluster_runung");
		job.setJarByClass(Connection.class);
		job.setMapperClass(ConnectionMapper.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, cos.getClassifiedFilePath());
		Path outPath = new Path(cos.getConnectionResultPath());
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
