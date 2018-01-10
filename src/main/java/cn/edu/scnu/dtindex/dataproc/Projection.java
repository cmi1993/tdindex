package cn.edu.scnu.dtindex.dataproc;


import cn.edu.scnu.dtindex.model.NoTime;
import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.model.ValidTime;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
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
import java.util.List;

/**
 * 分布式时态投影运算
 */
public class Projection {
	static CONSTANTS cos = CONSTANTS.getInstance();


	static class ProjectionMapper extends Mapper<LongWritable, Text, NoTime, ValidTime> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
			context.write(t.getNt(), t.getVt());
		}
	}

	static class ProjectionReducer extends Reducer<NoTime, ValidTime, Text, NullWritable> {
		@Override
		protected void reduce(NoTime key, Iterable<ValidTime> values, Context context) throws IOException, InterruptedException {
			String[] projectionFields = cos.getProjectionFields();
			StringBuilder newTuple = new StringBuilder();
			for (String field : projectionFields) {
				if (field.equals("uuid")) {
					newTuple.append(key.getUuid()).append(",");
				} else if (field.equals("name")) {
					newTuple.append(key.getName()).append(",");
				} else if (field.equals("mail")) {
					newTuple.append(key.getMail()).append(",");
				} else if (field.equals("cid")) {
					newTuple.append(key.getCid()).append(",");
				}
			}

			List<ValidTime> timelist = new ArrayList<ValidTime>();
			for (ValidTime tmp : values) {
				timelist.add(tmp);
			}

			Object[] times = timelist.toArray();
			ValidTime unionTime = ValidTime.union(times);
			context.write(new Text(newTuple.toString() + unionTime.toString()), NullWritable.get());

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name", "yarn");

		Job job = Job.getInstance(conf, "distributionProject_cluster_runung");
		job.setJar("/home/think/idea project/dtindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		job.setJarByClass(Sampler.class);
		job.setMapperClass(ProjectionMapper.class);
		job.setReducerClass(ProjectionReducer.class);
		job.setMapOutputKeyClass(NoTime.class);
		job.setMapOutputValueClass(ValidTime.class);
		FileInputFormat.setInputPaths(job, cos.getClassifiedFilePath());
		Path outPath = new Path(cos.getProjectionResultPath());
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
