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
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

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
			//List<Tuple> tupleList  = new CopyOnWriteArrayList<Tuple>();
			List<Tuple> tupleList = new ArrayList<Tuple>();
			for (String tupleStr : records) {
				String[] fields = tupleStr.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
			}
			System.out.println("[2]构建rTree索引-----------------------------------------------");
			RTreeNode root = new RTreeNode(0);//创建R树的根节点
			root.setIsroot(true);//标示为根节点
			RTree rtree = new RTree(root, tupleList.size() / 16, 16, tupleList.size());//初始化空的Rtree
			BuildRtree(tupleList, rtree, 0, root, rtree.getMaxSubtree(),rtree.getMaxNodeCapcity());//TGS构建Rtree
			MBR rootMBR = MBR.getInterNodeMBR(root.getNodeList());//计算根节点的MBR
			root.setMbr(rootMBR);//设置根节点的MBR

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

		/**
		 * 用于将叶节点的数据序列化出去，然后在R树的叶节点处记录一个偏移量来指向所在的数据
		 *
		 * @param node      用于序列化的节点，只有改节点是叶子节点的时候才进行序列化操作
		 * @param writer    用于序列化的写出类
		 * @param DiskKey   序列化对象的key
		 * @param DiskValue 序列化对象的value
		 * @throws IOException
		 */
		public void Serialization(RTreeNode node, SequenceFile.Writer writer, Text DiskKey, RTreeDiskSliceFile DiskValue) throws IOException {
			if (node.isLeaf()) {
				writer.append(new Text(""), new RTreeDiskSliceFile(node));
				long offset = writer.getLength();
				node.setOffset(offset);
				node.setIndex(true);
				node.clearLeafDataList();
			} else {
				if (node.getNodeList() != null) {
					List<RTreeNode> nodeList = node.getNodeList();
					for (RTreeNode n : nodeList) {
						Serialization(n, writer, DiskKey, DiskValue);
					}
				}
			}
		}


		/**
		 * 构建R树的算法
		 */
		public void BuildRtree(List<Tuple> tupleList, RTree tree, int buildTime, RTreeNode current, int maxSubTree,int splitcount) {

			if (tupleList.size() <= 2*tree.getMaxNodeCapcity()-1) {//如果数据条目再次进行分裂之后小于节点的半满，不进行分类，归结为一个叶节点
				current.setLeaf(true);
				current.setLeafDataSize(tupleList.size());
				return;
			} else {//递归进行数据切割算法
				List<RTreeSplitSlice> sclices = new ArrayList<RTreeSplitSlice>();//用于存储current节点分裂之后的数据容器
				List<RTreeNode> nodeList = new ArrayList<RTreeNode>();//用于存储current节点的孩子节点数据
				recursiveSplit(tupleList, tree, maxSubTree, splitcount, sclices);//进行分裂操作
				//将分裂成的n个切片作为current节点的孩子节点
				for (RTreeSplitSlice split : sclices) {
					RTreeNode node = new RTreeNode(buildTime, split.getSliceMBR(), split.getTuples());
					nodeList.add(node);
				}
				current.setNodeList(nodeList);
				current.setNodeSize(nodeList.size());
				//---------------------------------每个节点递归地进行分裂
				for (RTreeNode n : current.getNodeList()) {
					BuildRtree(n.getLeafData(), tree, buildTime++, n,
							(maxSubTree / tree.getMaxNodeCapcity()<tree.getMaxNodeCapcity()?//如果分裂之后导致节点不饱和，最大子树数量设置为阶数
									tree.getMaxNodeCapcity():(maxSubTree/tree.getMaxNodeCapcity())),
							(maxSubTree / tree.getMaxNodeCapcity()<tree.getMaxNodeCapcity()?//如果分裂之后导致节点不饱和，最大子树数量设置为阶数
									(int)(Math.ceil((double) maxSubTree/(double) tree.getMaxNodeCapcity())):tree.getMaxNodeCapcity()));
				}
				//清空非叶子节点的数据，避免冗余
				for (RTreeNode n : current.getNodeList()) {
					if (!n.isLeaf())
						n.clearLeafDataList();
				}
			}

		}

		/**
		 * @param tupleList--节点数量
		 * @param tree--用于构建的RTree
		 * @param perSubTreeNodeNum--此次分裂的子樹的节点数量
		 * @param splitNum--一开始为R树的阶数
		 * @return
		 */
		public void recursiveSplit(List<Tuple> tupleList, RTree tree, int perSubTreeNodeNum, int splitNum, List<RTreeSplitSlice> slices) {
			RTreeSplitSlice[] xSplit = XSplit(tupleList, tree, perSubTreeNodeNum, splitNum);
			RTreeSplitSlice[] ySplit = YSplit(tupleList, tree, perSubTreeNodeNum, splitNum);
			if (isBestToSplitInX) {//如果最佳分裂线在x上
				if (xSplit[0].getSplitCount() == 1) slices.add(xSplit[0]);
				else recursiveSplit(xSplit[0].getTuples(), tree, perSubTreeNodeNum, xSplit[0].getSplitCount(), slices);
				if (xSplit[1].getSplitCount() == 1) slices.add(xSplit[1]);
				else recursiveSplit(xSplit[1].getTuples(), tree, perSubTreeNodeNum, xSplit[1].getSplitCount(), slices);
				if ((xSplit[0].getSplitCount() == 1) && (xSplit[1].getSplitCount() == 1)) return;
			} else {//如果最佳分裂线在y上
				if (ySplit[0].getSplitCount() == 1) slices.add(ySplit[0]);
				else recursiveSplit(ySplit[0].getTuples(), tree, perSubTreeNodeNum, ySplit[0].getSplitCount(), slices);
				if (ySplit[1].getSplitCount() == 1) slices.add(ySplit[1]);
				else recursiveSplit(ySplit[1].getTuples(), tree, perSubTreeNodeNum, ySplit[1].getSplitCount(), slices);
				if ((ySplit[0].getSplitCount() == 1) && (ySplit[1].getSplitCount() == 1)) return;
			}
		}

		//x轴上寻找最佳分割线
		public RTreeSplitSlice[] XSplit(List<Tuple> tupleLists, RTree rTree, int perSubTreeNodeNum, int splitNum) {
			List<Tuple> tupleList = new ArrayList<Tuple>();
			tupleList.addAll(tupleLists);
			BigInteger minArea = null;
			RTreeSplitSlice[] slices = new RTreeSplitSlice[2];
			RTreeSplitContainer split = null;
			for (int i = 1; i < splitNum; i++) {
				int end = tupleList.size();
				List<Tuple> firstPart = tupleList.subList(0, i * perSubTreeNodeNum);
				List<Tuple> secondPart = tupleList.subList(i * perSubTreeNodeNum,end);
				MBR part1 = MBR.getTupleListMBR(firstPart);
				MBR part2 = MBR.getTupleListMBR(secondPart);
				if (i == 1) minArea = part1.mbrArea().add(part2.mbrArea());
				BigInteger thisArea = part1.mbrArea().add(part2.mbrArea());
				if (thisArea.compareTo(minArea) <= 0) {
					minArea = thisArea;
					isBestToSplitInX = true;
					bestSplitArea = thisArea;
					slices[0] = new RTreeSplitSlice(firstPart, i, part1);
					slices[1] = new RTreeSplitSlice(secondPart, splitNum - i, part2);
				}
			}
			return slices;
		}

		//y轴上寻找最佳分割线
		public RTreeSplitSlice[] YSplit(List<Tuple> tupleLists, RTree rTree, int perSubTreeNodeNum, int splitNum) {
			Collections.sort(tupleLists, new Comparator<Tuple>() {//Y排序比较器
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
			List<Tuple> tupleList = new ArrayList<Tuple>();
			tupleList.addAll(tupleLists);
			BigInteger minArea = bestSplitArea;
			RTreeSplitSlice[] slices = new RTreeSplitSlice[2];
			RTreeSplitContainer split = null;
			for (int i = 1; i < splitNum; i++) {
				int end = tupleList.size();
				List<Tuple> firstPart = tupleList.subList(0, i * perSubTreeNodeNum);
				List<Tuple> secondPart = tupleList.subList(i * perSubTreeNodeNum, end);
				MBR part1 = MBR.getTupleListMBR(firstPart);
				MBR part2 = MBR.getTupleListMBR(secondPart);
				BigInteger thisArea = part1.mbrArea().add(part2.mbrArea());
				if (thisArea.compareTo(minArea) <= 0) {
					minArea = thisArea;
					isBestToSplitInX = false;
					slices[0] = new RTreeSplitSlice(firstPart, i, part1);
					slices[1] = new RTreeSplitSlice(secondPart, splitNum - i, part2);
				}
			}
			return slices;
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
		local_running();
		//cluster_running();
	}

	public static void local_running() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "buildRTreeIndex_local_runung");


		job.setJarByClass(BuildRtreeIndex.class);
		job.setMapperClass(BuildRtreeIndexMapper.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(WholeFileInputFormat.class);

		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ByteWritable.class);
		FileInputFormat.setInputPaths(job, "/test/partitioner_123");
		//FileInputFormat.setInputPaths(job,cos.getClassifiedFilePath());
		Path outPath = new Path("/Users/think/Desktop/building_info/");//用于mr输出success信息的路径
		//Path outPath = new Path(cos.getDiskFilePath() + "/building_info/");//用于mr输出success信息的路径
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
		conf.set("mapreduce.job.jar", CONSTANTS.getMapReduceJobJarPath());
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
