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
			context.write(t, NullWritable.get());

		}
	}

	static class ClassifiedReducer extends Reducer<Tuple, NullWritable, Text, NullWritable> {
		@Override
		protected void reduce(Tuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			int partition = context.getConfiguration().getInt("mapred.task.partition", -1);//获取分区号
			context.write(new Text(key.toString()), NullWritable.get());
		}
	}

	static class DataPatitioner extends Partitioner<Tuple, NullWritable> {
		static long[] xparts = cos.getxPatitionsData();
		static long[][] yparts = cos.getyPatitionsData();

		@Override
		public int getPartition(Tuple tuple, NullWritable nullWritable, int i) {
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
			if (start <= xparts[1]) return 0;
			else if (start > xparts[xparts.length - 2])
				return xparts.length - 2;
			else {
				for (int i = 1; i < xparts.length - 2; i++) {
					if (start > xparts[i] && start <= xparts[i + 1])
						return i;
				}
			}
			return -1;//找不到
		}

		private int SeekYPatition(int xpartOrder, long end) {
			if (end <= yparts[xpartOrder][1]) return 0;
			else if (end > yparts[xpartOrder][yparts[xpartOrder].length - 2])
				return yparts[xpartOrder].length - 2;
			else {
				for (int i = 1; i <= yparts[xpartOrder].length - 2; i++) {
					if (end > yparts[xpartOrder][i] && end <= yparts[xpartOrder][i + 1])
						return i;
				}
			}
			return -1;//找不到
		}

	}


	static class ClassifiedOutputFormat extends FileOutputFormat<Text,NullWritable>{
		@Override
		public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
			FileSystem fs =  FileSystem.get(context.getConfiguration());
			int partition = context.getConfiguration().getInt("mapred.task.partition",-1);
			String spath = "/home/think/Desktop/data/classifiedData/partitioner_"+partition;
			Path path = new Path(spath);
			FSDataOutputStream fsout = fs.create(path);
			return new ClassifedRecordWriter(fsout);
		}
	}

	static class ClassifedRecordWriter extends RecordWriter<Text,NullWritable>{
		FSDataOutputStream outputStream = null;

		public ClassifedRecordWriter(FSDataOutputStream outputStream) {
			this.outputStream = outputStream;
		}

		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			outputStream.writeUTF(key.toString());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			if (outputStream!=null)
				outputStream.close();
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//numOfReducer = cos.getNumOfEachdimention()*cos.getNumOfEachdimention();
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "classified_local");


		job.setJarByClass(ClassifiedDataIntoSlice.class);
		job.setMapperClass(ClassifiedMapper.class);
		job.setReducerClass(ClassifiedReducer.class);
		job.setOutputFormatClass(ClassifiedOutputFormat.class);
		job.setNumReduceTasks(cos.getNumOfXDimention() * cos.getNumOfYDimention());
		job.setPartitionerClass(DataPatitioner.class);
// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Tuple.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, CONSTANTS.getDataFilePath());
		Path outPath = new Path(CONSTANTS.getClassifiedFilePath());
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
