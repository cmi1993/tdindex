package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.*;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.List;

public class ReadingIndex {

	static CONSTANTS cos;

	static {
		try {
			cos = CONSTANTS.readPersistenceData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static class ReadingIndexMapper extends Mapper<Text, DiskSliceFile, IntWritable, IndexRecord> {

		@Override
		protected void map(Text key, DiskSliceFile value, Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();//获取当前切片
			Path path = ((FileSplit) split).getPath();//获取切片路径
			String filename = path.getName();
			String[] splits = filename.split("_");
			int patitionId = Integer.parseInt(splits[2]);

			ValidTime queryWindow = new ValidTime(CONSTANTS.getQueryStart(), CONSTANTS.getQueryEnd());
			IndexFile index = value.getIndex();
			List<IndexRecord> records = index.getRecords();
			for (IndexRecord indexRecord : records) {
				if (queryWindow.isMisPlace(indexRecord.getMinNode()))
					continue;
				else {
					indexRecord.setDiskFileName(filename);
					System.out.println(indexRecord.toString());
					context.write(new IntWritable(patitionId), indexRecord);
				}
			}
		}
	}

	static class ReadingIndexPatitioner extends Partitioner<IntWritable, IndexRecord> {
		@Override
		public int getPartition(IntWritable intWritable, IndexRecord indexRecord, int numPartitions) {
			int id = intWritable.get();
			return id;
		}
	}

	static class ReadingIndexReducer extends Reducer<IntWritable, IndexRecord, Text, NullWritable> {
		@Override
		protected void reduce(IntWritable key, Iterable<IndexRecord> values, Context context) throws IOException, InterruptedException {
			int partitionId = key.get();
			String filename = values.iterator().next().getDiskFileName();
			Path partitionDataPath = new Path(filename);
			Text Diskkey = new Text();
			DiskSliceFile DiskValue = new DiskSliceFile();
			Configuration conf = new Configuration();
			SequenceFile.Reader reader = null;
			reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(partitionDataPath));


			for (IndexRecord value : values) {
				Long lob_offset = value.getLob_offset();
				reader.seek(lob_return 0; offset);
				boolean next = reader.next(Diskkey, DiskValue);


			}
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		cos.setQueryStart("1979-12-30 00:00:04");
		cos.setQueryEnd("1997-04-17 13:54:40");


		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "readIndex_local");


		job.setNumReduceTasks(cos.getNumOfYDimention() * cos.getNumOfXDimention());
		job.setJarByClass(ReadingIndex.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setReducerClass(ReadingIndexReducer.class);
		job.setPartitionerClass(ReadingIndexPatitioner.class);
		job.setMapperClass(ReadingIndexMapper.class);
		SequenceFileInputFormat.addInputPath(job, new Path(CONSTANTS.getIndexFileDir() + "/index_small.txt.seq"));
		Path outPath = new Path("/home/think/Desktop/data/queryInfo/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
