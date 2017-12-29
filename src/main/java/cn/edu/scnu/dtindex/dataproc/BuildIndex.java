package cn.edu.scnu.dtindex.dataproc;


import cn.edu.scnu.dtindex.model.*;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BuildIndex {
	static class WholeFileInputFormat extends FileInputFormat<Text, Text> {
		@Override
		protected boolean isSplitable(JobContext context, Path filename) {
			return false;
		}

		@Override
		public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			RecordReader<Text, Text> recordReader = new WholeFileRecordReader();
			return recordReader;
		}
	}

	static class WholeFileRecordReader extends RecordReader<Text, Text> {
		private FileSplit fileSplit;
		private Configuration conf;
		private JobContext jobContext;
		private Text currentKey = new Text();
		private Text currentValue = new Text();
		private boolean processed = false;


		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			this.fileSplit = (FileSplit) split;
			this.jobContext = context;
			String filename = fileSplit.getPath().getName();
			this.currentKey = new Text(filename);

		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (!processed) {
				int len = (int) fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
				FSDataInputStream in = fs.open(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
				StringBuilder str = new StringBuilder();
				String line = "";
				while ((line = br.readLine()) != null) {
					str.append(line).append("\n");
				}
				br.close();
				in.close();
				fs.close();
				currentValue = new Text(str.toString());
				processed = true;
				return true;

			}
			return false;
		}

		@Override
		public Text getCurrentKey() throws IOException, InterruptedException {
			return currentKey;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return currentValue;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			float progress = 0;
			if (processed) {
				progress = 1;
			}
			return progress;
		}

		@Override
		public void close() throws IOException {

		}
	}

	static class BuildIndexMapper extends Mapper<Text, Text, Text, BytesWritable> {
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("[1]读取单一分区的有序数据-----------------------------------------------");
			String[] records = value.toString().split("\n");
			List<Tuple> tupleList = new ArrayList<Tuple>();
			for (String tupleStr : records) {
				String[] fields = tupleStr.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
			}
			List<Lob> LobList = new ArrayList<Lob>();
			System.out.println("[2]进行线序划分算法构建内存中的索引--------------------------------------");
			int length = tupleList.size();
			List<Lob> result = new ArrayList<Lob>();
			List<Tuple> tempSubList = null;

			Tuple temp = null;
			Tuple finder = null;
			for (int i = 0; i < length; ) {//i表示已经划分完的节点数
				temp = tupleList.get(0);
				i++;
				tempSubList = new ArrayList<Tuple>();
				tempSubList.add(temp);
				tupleList.remove(0);//去除第一个元素

				if (i == length) {//最后一个元素单独组成lob
					result.add(new Lob(tempSubList));
				}

				int subSize = tupleList.size();
				for (int j = 0; j < subSize; j++) {
					finder = tupleList.get(j);
					if (finder.getVt().getStart() >= temp.getVt().getStart()
							&& finder.getVt().getEnd() <= temp.getVt().getEnd()) {
						tempSubList.add(finder);
						i++;
						temp = finder;
						tupleList.remove(j);
						subSize--;
						j--;
					}
					if (finder.getVt().getStart() > tempSubList.get(0).getVt().getEnd())
						break;
				}
				result.add(new Lob(tempSubList));
			}
			System.out.println("[3]构建用于序列化存储的磁盘块-------------------------------------------");
			InputSplit inputSplit=(InputSplit)context.getInputSplit();
			String filename=((FileSplit)inputSplit).getPath().getName();
			Text Diskkey = new Text();
			DiskSliceFile DiskValue = new DiskSliceFile();
			Configuration conf = new Configuration();
			MapWritable indexMap = new MapWritable();

			SequenceFile.Writer writer =
					SequenceFile.createWriter(conf,
							SequenceFile.Writer.file(new Path(CONSTANTS.getDiskFilePath() + "/disk_"+filename+".seq")),
							SequenceFile.Writer.keyClass(Diskkey.getClass()),
							SequenceFile.Writer.valueClass(DiskValue.getClass()),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
			System.out.println("[3.1]准备序列化数据块------------------");
			for (Lob lobdata : result) {
				Diskkey = new Text("lobid:" + lobdata.getLobid());
				DiskValue = new DiskSliceFile(lobdata);
				long lobOffset = writer.getLength();
				IndexRecord lobIndex = new IndexRecord(lobdata.getLobid(), lobdata.getMaxNode(), lobdata.getMinNode(), lobOffset);
				indexMap.put(Diskkey, DiskValue);
				writer.append(Diskkey, DiskValue);
			}
			System.out.println("[3.2]准备序列化索引--------------------");
			long indexBegin = writer.getLength();
			IndexFile idx = new IndexFile(indexMap, indexBegin);
			System.out.println("[3.3]开始序列化数据--------------------");
			IOUtils.closeStream(writer);
			System.out.println("[3.4]序列化数据成功--------------------");

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "buildIndex_local");


		job.setJarByClass(BuildIndex.class);

		job.setNumReduceTasks(0);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(BuildIndexMapper.class);
		FileInputFormat.setInputPaths(job, "/home/think/Desktop/data/small");
		Path outPath = new Path("/home/think/Desktop/data/index");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
