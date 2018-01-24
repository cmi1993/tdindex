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
			cos = CONSTANTS.getInstance().readPersistenceData();
			cos.setQueryStart("1979-12-30 00:00:04");
			cos.setQueryEnd("1997-04-17 13:54:40");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	static class ReadingIndexMapper extends Mapper<Text, DiskSliceFile, IntWritable, IndexRecord> {

		@Override
		protected void map(Text key, DiskSliceFile value, Context context) throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();//获取当前切片
			Path path = ((FileSplit) split).getPath();//获取切片路径
			String filename = path.getName();//index_partitioner_0_1009478186.seq
			System.out.println("【filename:】"+filename);
			String[] splits = filename.split("_");
			int patitionId = Integer.parseInt(splits[2]);
			System.out.println("【partitionId:】"+patitionId);
			ValidTime queryWindow = new ValidTime(cos.getQueryStart(), cos.getQueryEnd());
			System.out.println("【query window:】"+queryWindow.toString());
			IndexFile index = value.getIndex();
			List<IndexRecord> records = index.getRecords();
			System.out.println("records size:"+records.size());
			for (IndexRecord indexRecord : records) {
				if (queryWindow.isMisPlace(indexRecord.getMinNode()))
					continue;
				else {
					indexRecord.setDiskFileName(filename);
					//System.out.println(indexRecord.toString());
					context.write(new IntWritable(patitionId), indexRecord);//按分区号写出所需查询的索引
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
			System.out.println("partitionId:"+partitionId);
			String indexFilename = values.iterator().next().getDiskFileName();//index_partitioner_0_1009478186.seq
			System.out.println("indexFilename"+indexFilename);
			//index_partitioner_0_1009478186.seq-->disk_partitioner_0.seq;
			String[] fields = indexFilename.split("_");
			String diskFilename = "disk_partitioner_" + fields[2] + ".seq";
			Path partitionDataPath = new Path(cos.getDiskSliceFileDir()+"/"+diskFilename);

			Text Diskkey = new Text();
			DiskSliceFile DiskValue = new DiskSliceFile();
			Configuration conf = new Configuration();
			SequenceFile.Reader reader = null;
			ValidTime query = new ValidTime(cos.getQueryStart(), cos.getQueryEnd());
			reader = new SequenceFile.Reader(context.getConfiguration(), SequenceFile.Reader.file(partitionDataPath));


			for (IndexRecord value : values) {
				Long lob_offset = value.getLob_offset();
				reader.seek(lob_offset);
				boolean next = reader.next(Diskkey, DiskValue);
				List<Tuple> tuples = DiskValue.getData().BinarySearchInLob(query);
				StringBuilder queryResult = new StringBuilder();
				for (Tuple t : tuples) {
					queryResult.append(t.toString()).append("\n");
				}
				context.write(new Text(queryResult.toString()), NullWritable.get());

			}
		}
	}


	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("fs.default", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		//System.setProperty("HADOOP_USER_NAME", "root");

		conf.set("mapreduce.job.jar", "/Users/think/Library/Mobile Documents/com~apple~CloudDocs/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");

		Job job = Job.getInstance(conf, "readIndex_cluster");


		job.setNumReduceTasks(cos.getNumOfYDimention() * cos.getNumOfXDimention());
		job.setJarByClass(ReadingIndex.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IndexRecord.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//-----------------------------------------------------------------------
		job.setReducerClass(ReadingIndexReducer.class);
		job.setPartitionerClass(ReadingIndexPatitioner.class);
		job.setMapperClass(ReadingIndexMapper.class);
		SequenceFileInputFormat.addInputPath(job, new Path(cos.getIndexFileDir() + "/"));
		Path outPath = new Path(cos.getDataFileDir() + "/queryInfo/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
