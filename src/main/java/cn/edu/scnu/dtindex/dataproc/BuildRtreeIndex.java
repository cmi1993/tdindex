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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;

public class BuildRtreeIndex {
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

	static class BuildRtreeIndexMapper extends Mapper<Text, Text, Text, BytesWritable> {
		public static BigInteger bestSplitArea;
		public static int bestSplitPos;
		public static boolean isBestToSplitInX;

		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("[1]读取单一分区的x有序数据-----------------------------------------------");
			String[] records = value.toString().split("\n");
			List<Tuple> tupleList = new ArrayList<Tuple>();
			for (String tupleStr : records) {
				String[] fields = tupleStr.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
			}
			System.out.println("[2]构建rTree索引-----------------------------------------------");
			RTreeNode root = new RTreeNode(0);
			root.setIsroot(true);
			RTree rtree = new RTree(root, tupleList.size() / 4, 4, tupleList.size());
			BuildRtree(tupleList, rtree, 0, root, rtree.getMaxSubtree());
			MBR rootMBR = MBR.getInterNodeMBR(root.getNodeList());
			root.setMbr(rootMBR);

			System.out.println("[3]准备序列化数据-----------------------------------------------");
			InputSplit inputSplit = context.getInputSplit();
			String filename = ((FileSplit) inputSplit).getPath().getName();
			Text DiskKey = new Text();
			RTreeDiskSliceFile DiskValue = new RTreeDiskSliceFile();
			Configuration conf = new Configuration();
			SequenceFile.Writer writer =
					SequenceFile.createWriter(conf,
							SequenceFile.Writer.file(new Path(cos.getDiskFilePath() + "/RTree_disk/disk_" + filename + ".seq")),
							SequenceFile.Writer.keyClass(DiskKey.getClass()),
							SequenceFile.Writer.valueClass(DiskValue.getClass()),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
			Serialization(root, writer, DiskKey, DiskValue);
			long indexBegin = writer.getLength();
			rtree.setIndexBeginOffset(indexBegin);

			writer.append(new Text(""), new RTreeDiskSliceFile(rtree));

			System.out.println("[4]开始序列化数据-----------------------------------------------");
			IOUtils.closeStream(writer);
			System.out.println("[5]序列化数据成功-----------------------------------------------");
			writer =
					SequenceFile.createWriter(conf,
							SequenceFile.Writer.file(new Path(cos.getDiskFilePath() + "/RTreeIndex/RTreeindex_" + filename + "_" + indexBegin + ".seq")),
							SequenceFile.Writer.keyClass(DiskKey.getClass()),
							SequenceFile.Writer.valueClass(DiskValue.getClass()),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
			RTreeDiskSliceFile indexFile = new RTreeDiskSliceFile(rtree);
			writer.append(new Text("index"), indexFile);
			IOUtils.closeStream(writer);
			System.out.println("[6]序列化索引成功-----------------------------------------------");
			System.out.println("success!!");
		}

		public void Serialization(RTreeNode node, SequenceFile.Writer writer, Text DiskKey, RTreeDiskSliceFile DiskValue) throws IOException {
			if (node.isLeaf()) {
				writer.append(new Text(""), new RTreeDiskSliceFile(node));
				long offset = writer.getLength();
				node.setOffset(offset);
				node.setIndex(true);
				node.clearLeafDataList();
			} else {
				List<RTreeNode> nodeList = node.getNodeList();
				for (RTreeNode n : nodeList) {
					Serialization(n, writer, DiskKey, DiskValue);
				}
			}
		}

		static int splitcount = 0;
		static Stack<RTreeSplitContainer> splitStack = new Stack<RTreeSplitContainer>();
		static Stack<RTreeSplitContainer> tmpStack;

		public void BuildRtree(List<Tuple> tupleList, RTree tree, int buildTimes, RTreeNode current, int maxSubTree) {
			if (tupleList.size() <= tree.getMaxNodeCapcity()) {
				current.setLeaf(true);
				current.setLeafDataSize(tupleList.size());
				return;
			} else {
				tmpStack = new Stack<RTreeSplitContainer>();
				RTreeSplitContainer splits = recursiveSplit(tupleList, tree);
				splitStack.push(splits);
				splitcount = 0;
				while (splitcount != tree.getMaxNodeCapcity()) {//不断分裂，直到可以足够填满一层
					for (RTreeSplitContainer container : splitStack) {
						RTreeSplitContainer container1 = recursiveSplit(container.getPart1(), tree);
						RTreeSplitContainer container2 = recursiveSplit(container.getPart2(), tree);
						tmpStack.push(container1);
						tmpStack.push(container2);
					}
					splitStack.clear();
					splitStack = tmpStack;
				}
				//---------------------------------开始填充节点
				List<RTreeNode> nodeList = new ArrayList<RTreeNode>();
				while (!splitStack.empty()) {
					RTreeSplitContainer pop = splitStack.pop();
					RTreeNode node1 = new RTreeNode(0, pop.getPart1MBR(), pop.getPart1());
					nodeList.add(node1);
					RTreeNode node2 = new RTreeNode(0, pop.getPart2MBR(), pop.getPart2());
					nodeList.add(node2);
				}
				current.setNodeList(nodeList);
				current.setNodeSize(nodeList.size());


				//---------------------------------每个节点递归地进行分裂
				for (RTreeNode n : current.getNodeList()) {
					BuildRtree(((RTreeNode) n).getLeafData(), tree, buildTimes++, ((RTreeNode) n), maxSubTree / tree.getMaxNodeCapcity());

				}

				for (RTreeNode n : current.getNodeList()) {
					if (!((RTreeNode) n).isLeaf())
						((RTreeNode) n).clearLeafDataList();
				}
			}

		}

		public RTreeSplitContainer recursiveSplit(List<Tuple> tupleList, RTree tree) {
			splitcount += 2;
			//x dimention find //x维度寻找最佳扫描线
			RTreeSplitContainer xSplit = XSplit(tupleList, tree, tupleList.size() / tree.getMaxNodeCapcity());
			//y dimention find //y维度寻找最佳扫描线
			RTreeSplitContainer ySplit = YSplit(tupleList, tree, tupleList.size() / tree.getMaxNodeCapcity());
			if (isBestToSplitInX)
				return xSplit;
			else
				return ySplit;
		}

		public RTreeSplitContainer YSplit(List<Tuple> tupleList, RTree rTree, int perSubTreeNodeNum) {
			Collections.sort(tupleList, new Comparator<Tuple>() {//Y排序比较器
				@Override
				public int compare(Tuple o1, Tuple o2) {
					if (o1.getVt().getEnd() > o2.getVt().getEnd())
						return 1;
					else if (o1.getVt().getEnd() < o2.getVt().getEnd())
						return -1;
					else
						return 0;
				}
			});
			BigInteger minArea = bestSplitArea;
			List<Tuple>[] twopart = new List[2];
			RTreeSplitContainer split = null;
			//fisrt Part---0-i*S
			List<Tuple> firstPart = tupleList.subList(0, tupleList.size() / 2);
			//the other Part
			List<Tuple> secondPart = tupleList.subList(tupleList.size() / 2, tupleList.size());
			MBR part1 = MBR.getTupleListMBR(firstPart);
			MBR part2 = MBR.getTupleListMBR(secondPart);
			BigInteger thisArea = part1.mbrArea().add(part2.mbrArea());
			if (thisArea.compareTo(minArea) <= 0) {
				minArea = thisArea;
				isBestToSplitInX = false;
				bestSplitArea = minArea;
				split = new RTreeSplitContainer(firstPart, secondPart, part1, part2);
			}
			return split;
		}

		public RTreeSplitContainer XSplit(List<Tuple> tupleList, RTree rTree, int perSubTreeNodeNum) {
			BigInteger minArea = null;
			List<Tuple>[] twopart = new List[2];
			RTreeSplitContainer split = null;
			//fisrt Part---0-i*S
			List<Tuple> firstPart = tupleList.subList(0, tupleList.size() / 2);
			//the other Part
			List<Tuple> secondPart = tupleList.subList(tupleList.size() / 2, tupleList.size());
			MBR part1 = MBR.getTupleListMBR(firstPart);
			MBR part2 = MBR.getTupleListMBR(secondPart);
			BigInteger thisArea = part1.mbrArea().add(part2.mbrArea());
			minArea = new BigInteger(String.valueOf(thisArea));
			if (thisArea.compareTo(minArea) <= 0) {
				minArea = thisArea;
				//bestSplitPos = i * perSubTreeNodeNum;
				isBestToSplitInX = true;
				bestSplitArea = minArea;
				List<Tuple> fisrt = new ArrayList<Tuple>();
				fisrt.addAll(firstPart);
				List<Tuple> sec = new ArrayList<Tuple>();
				sec.addAll(secondPart);
				split = new RTreeSplitContainer(fisrt, sec, part1, part2);
			}
			return split;
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
				int len = (int) fileSplit.getLength();
				Path file = fileSplit.getPath();
				FileSystem fs = file.getFileSystem(jobContext.getConfiguration());
				FSDataInputStream in = fs.open(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
				String line = "";
				StringBuilder total = new StringBuilder(len);
				while ((line = br.readLine()) != null) {
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

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//local_running();
		cluster_running();
	}

	public static void local_running() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		System.setProperty("HADOOP_USER_NAME", "root");
		Job job = Job.getInstance(conf, "buildRTreeIndex_local_runung");


		job.setJarByClass(BuildRtreeIndex.class);
		job.setMapperClass(BuildRtreeIndexMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(WholeFileInputFormat.class);

		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteWritable.class);
		FileInputFormat.setInputPaths(job, "/test/1/1.txt");
		//FileInputFormat.setInputPaths(job,cos.getClassifiedFilePath());
		Path outPath = new Path(cos.getDiskFilePath() + "/building_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// 向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		CONSTANTS.persistenceData(cos);
		System.exit(res ? 0 : 1);
	}

	public static void cluster_running() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.scheduler.minimum-allocation-mb", "1024");
		conf.set("yarn.scheduler.maximum-allocation-mb", "16384");
		conf.set("yarn.nodemanager.resource.memory-mb", "200000");
		conf.set("mapreduce.map.memory.mb", "4096");
		conf.set("yarn.scheduler.minimum-allocation-vcore", "10");
		conf.set("yarn.scheduler.maximum-allocation-vcore", "20");
		//conf.set("mapreduce.map.java.opts","-Xmx2048");
		//conf.set("mapreduce.reduce.memory.mb","4096");
		conf.set("mapreduce.map.cpu.vcore", "10");
		//conf.set("yarn.node-manager.resource.vcore","20");
		//conf.set("yarn.resourcemanager.hostname", "root");
		//conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		System.setProperty("HADOOP_USER_NAME", "root");
		conf.set("mapreduce.job.jar", "/Users/think/Library/Mobile Documents/com~apple~CloudDocs/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Job job = Job.getInstance(conf, "buildRTreeIndex_cluster_runung");


		job.setJarByClass(BuildRtreeIndex.class);
		job.setMapperClass(BuildRtreeIndexMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(WholeFileInputFormat.class);

		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteWritable.class);
		//FileInputFormat.setInputPaths(job, "/test/1/1.txt");
		FileInputFormat.setInputPaths(job, cos.getClassifiedFilePath());
		Path outPath = new Path(cos.getDiskFilePath() + "/building_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// 向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		CONSTANTS.persistenceData(cos);
		System.exit(res ? 0 : 1);
	}
}
