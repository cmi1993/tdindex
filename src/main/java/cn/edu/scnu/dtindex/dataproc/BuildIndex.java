
package cn.edu.scnu.dtindex.dataproc;


import cn.edu.scnu.dtindex.model.*;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import cn.edu.scnu.dtindex.tools.DFSIOTools;
import cn.edu.scnu.dtindex.tools.HDFSTool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BuildIndex {
	private static Log log = LogFactory.getLog(BuildIndex.class);
	static CONSTANTS cos;

	static {
		try {
			 cos = CONSTANTS.readPersistenceData();
			 cos.showConstantsInfo();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
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
				int len = (int)fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
				FSDataInputStream in = fs.open(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in,"utf-8"));
				String line="";
				StringBuilder total = new StringBuilder(len);
				while((line= br.readLine())!= null){
					total.append(line).append("\n");
				}
				br.close();
				in.close();
				fs.close();
				//String total = DFSIOTools.toReadWithCharReturn(jobContext.getConfiguration(), fileSplit.getPath().getName());
				currentValue = new Text(total.toString());
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
			log.info("[1]读取单一分区的有序数据-----------------------------------------------");
			System.out.println("[1]读取单一分区的有序数据-----------------------------------------------");
			String[] records = value.toString().split("\n");
			List<Tuple> tupleList = new ArrayList<Tuple>();
			for (String tupleStr : records) {
				String[] fields = tupleStr.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
			}
			List<Lob> LobList = new ArrayList<Lob>();
			System.out.println("记录的个数："+tupleList.size());
			System.out.println("[2]进行线序划分算法构建内存中的索引--------------------------------------");
			int length = tupleList.size();
			List<Lob> result = new ArrayList<Lob>();
			List<Tuple> tempSubList = null;

			Tuple temp = null;
			Tuple finder = null;
			System.out.println("hello");
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
			InputSplit inputSplit = (InputSplit) context.getInputSplit();
			String filename = ((FileSplit) inputSplit).getPath().getName();
			Text Diskkey = new Text();
			DiskSliceFile DiskValue = new DiskSliceFile();
			Configuration conf = new Configuration();
			List<IndexRecord> indexRecordList = new ArrayList<IndexRecord>();
			SequenceFile.Writer writer =
					SequenceFile.createWriter(conf,
							SequenceFile.Writer.file(new Path(cos.getDiskFilePath() + "/disk/disk_" + filename + ".seq")),
							SequenceFile.Writer.keyClass(Diskkey.getClass()),
							SequenceFile.Writer.valueClass(DiskValue.getClass()),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
			System.out.println("[3.1]准备序列化数据块------------------");
			for (Lob lobdata : result) {
				Diskkey = new Text("lobid:" + lobdata.getLobid());
				DiskValue = new DiskSliceFile(lobdata);
				long lobOffset = writer.getLength();
				IndexRecord lobIndex = new IndexRecord(lobdata.getLobid(), lobdata.getMaxNode(), lobdata.getMinNode(), lobOffset);
				indexRecordList.add(lobIndex);
				writer.append(Diskkey, DiskValue);
			}
			System.out.println("[3.2]准备序列化索引--------------------");
			long indexBegin = writer.getLength();
			IndexFile idx = new IndexFile(indexRecordList, indexBegin);
			writer.append(new Text("index"), new DiskSliceFile(idx));
			System.out.println("[3.3]开始序列化数据--------------------");
			IOUtils.closeStream(writer);
			System.out.println("[3.4]序列化数据成功--------------------");
			//TODO:文件操作api没有改成分布式
			/*HDFSTool tools = new HDFSTool(context.getConfiguration());
			if (tools.isExits(cos.getDiskSliceFileDir() + "/disk_" + filename + ".seq")){
				tools.renameFile(cos.getDiskSliceFileDir() + "/disk_" + filename + ".seq",
						cos.getDiskSliceFileDir() + "/disk_" + filename + "_" + indexBegin + ".seq");
				tools.rmr(cos.getDiskSliceFileDir() + "/disk_" + filename + ".seq");
			}*/

			writer =
					SequenceFile.createWriter(conf,
							SequenceFile.Writer.file(new Path(cos.getIndexFileDir() + "/index_" + filename +"_"+indexBegin +".seq")),
							SequenceFile.Writer.keyClass(Diskkey.getClass()),
							SequenceFile.Writer.valueClass(DiskValue.getClass()),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
			writer.append(new Text("index"), new DiskSliceFile(idx));
			IOUtils.closeStream(writer);
			System.out.println("[3.5]序列化索引成功--------------------");

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.scheduler.minimum-allocation-mb","1024");
		conf.set("yarn.scheduler.maximum-allocation-mb","16384");
		conf.set("yarn.nodemanager.resource.memory-mb","200000");
		conf.set("mapreduce.map.memory.mb","4096");
		conf.set("yarn.scheduler.minimum-allocation-vcore","10");
		conf.set("yarn.scheduler.maximum-allocation-vcore","20");
		//conf.set("mapreduce.map.java.opts","-Xmx2048");
		//conf.set("mapreduce.reduce.memory.mb","4096");
		conf.set("mapreduce.map.cpu.vcore","10");
		//conf.set("yarn.node-manager.resource.vcore","20");
		//conf.set("yarn.resourcemanager.hostname", "root");
		//conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		System.setProperty("HADOOP_USER_NAME", "root");
		conf.set("mapreduce.job.jar","/home/think/idea project/dtindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Job job = Job.getInstance(conf, "buildIndex_cluster_runung");



		job.setJarByClass(BuildIndex.class);

		job.setNumReduceTasks(0);
		job.setInputFormatClass(WholeFileInputFormat.class);
		//job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(BuildIndexMapper.class);
		//FileInputFormat.setInputPaths(job,CONSTANTS.getClassifiedFilePath());
		FileInputFormat.setInputPaths(job,cos.getClassifiedFilePath());
		Path outPath = new Path(cos.getDiskFilePath()+"/building_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
