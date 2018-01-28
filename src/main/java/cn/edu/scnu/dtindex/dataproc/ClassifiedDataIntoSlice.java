package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ClassifiedDataIntoSlice {
	static CONSTANTS cos;


	static {
		try {
			 cos = CONSTANTS.readPersistenceData();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}


	static class ClassifiedMapper extends Mapper<LongWritable, Text, Tuple, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] splits = line.split(",");
			Tuple t = new Tuple(splits[0], splits[1], splits[2], splits[3], splits[4], splits[5]);
			System.out.println(t.toString());
			context.write(t, NullWritable.get());

		}
	}

	static class ClassifiedReducer extends Reducer<Tuple, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Tuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(new Text(key.toString()), NullWritable.get());
		}
	}

	static class DataPatitioner extends Partitioner<Tuple, NullWritable> {


		@Override
		public int getPartition(Tuple tuple, NullWritable nullWritable, int i) {
			System.out.println(tuple.toString());
			int xpartOrder = SeekXPatition(tuple.getVt().getStart());
			if (xpartOrder == -1) {
				System.out.println("查找x分区出错:");
				System.exit(1);
			}
			int ypartOrder = SeekYPatition(xpartOrder, tuple.getVt().getEnd());
			if (ypartOrder == -1) {
				System.out.println("查找y分区出错:" + tuple.toString());
				System.exit(1);
			}
			return xpartOrder * cos.getNumOfYDimention() + ypartOrder;
		}


		private int SeekXPatition(long start) {
			if (start <= cos.getxPatitionsData()[1]) return 0;
			else if (start > cos.getxPatitionsData()[cos.getxPatitionsData().length - 2])
				return cos.getxPatitionsData().length - 2;
			else {
				for (int i = 1; i < cos.getxPatitionsData().length - 2; i++) {
					if (start > cos.getxPatitionsData()[i] && start <= cos.getxPatitionsData()[i + 1])
						return i;
				}
			}
			return -1;//找不到
		}

		private int SeekYPatition(int xpartOrder, long end) {
			if (end <= cos.getyPatitionsData()[xpartOrder][1]) return 0;
			else if (end > cos.getyPatitionsData()[xpartOrder][cos.getyPatitionsData()[xpartOrder].length - 2])
				return cos.getyPatitionsData()[xpartOrder].length - 2;
			else {
				for (int i = 1; i <= cos.getyPatitionsData()[xpartOrder].length - 2; i++) {
					if (end > cos.getyPatitionsData()[xpartOrder][i] && end <= cos.getyPatitionsData()[xpartOrder][i + 1])
						return i;
				}
			}
			return -1;//找不到
		}

	}


	static class ClassifiedOutputFormat extends FileOutputFormat<Text, NullWritable> {
		@Override
		public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			int partition = context.getConfiguration().getInt("mapred.task.partition", -1);
			String spath = cos.getClassifiedFilePath() + "/partitioner_" + partition;
			Path path = new Path(spath);
			FSDataOutputStream fsout = fs.create(path);
			return new ClassifedRecordWriter(fsout);
		}
	}

	static class ClassifedRecordWriter extends RecordWriter<Text, NullWritable> {
		FSDataOutputStream outputStream = null;

		public ClassifedRecordWriter(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			outputStream.write((key.toString() + "\n").getBytes());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if (outputStream != null)
				outputStream.close();
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default", "hdfs://192.168.69.204:8020/");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "root");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		System.setProperty("HADOOP_USER_NAME", "root");

		conf.set("mapreduce.job.jar", "/Users/think/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");

		Job job = Job.getInstance(conf, "classified_cluster_runung");
		//job.setJar("/home/think/idea project/dtindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		//job.setJar("/root/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");

		job.setJarByClass(ClassifiedDataIntoSlice.class);
		job.setMapperClass(ClassifiedMapper.class);

		job.setOutputFormatClass(ClassifiedOutputFormat.class);
		int reducerNum = cos.getNumOfXDimention() * cos.getNumOfYDimention();
		job.setNumReduceTasks(reducerNum);
		job.setReducerClass(ClassifiedReducer.class);
		job.setPartitionerClass(DataPatitioner.class);

		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		System.out.println(cos.getDataFilePath());
		FileInputFormat.setInputPaths(job, cos.getDataFilePath());
		//FileInputFormat.setInputPaths(job,"/home/think/Desktop/data/small.txt");
		Path outPath = new Path(cos.getClassifiedFilePath());
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
