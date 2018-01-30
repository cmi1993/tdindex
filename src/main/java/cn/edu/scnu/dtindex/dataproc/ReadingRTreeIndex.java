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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadingRTreeIndex {
	static CONSTANTS cos;

	static {
		try {
			cos = CONSTANTS.getInstance().readPersistenceData();
			cos.setQueryStart("1979-12-30 00:00:04");
			cos.setQueryEnd("2000-04-17 13:54:40");
			//cos.showConstantsInfo();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static class ReadingRTreeIndexMapper extends Mapper<Text, RTreeDiskSliceFile, IntWritable, RTree> {
		@Override
		protected void map(Text key, RTreeDiskSliceFile value, Context context) throws IOException, InterruptedException {
			System.out.println("********************************************************************");
			System.out.println("*                               Mapper阶段                          *");
			System.out.println("********************************************************************");
			System.out.println("获取分区编号和文件名称");
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
			String filename = path.getName();//获取分区磁盘块名称
			System.out.println("【filename】:" + filename);
			String[] splits = filename.split("_");
			int partitionId = Integer.parseInt(splits[2]);//获取分区id
			System.out.println("【partitionId】：" + partitionId);

			ValidTime queryWindow = new ValidTime(cos.getQueryStart(), cos.getQueryEnd());
			System.out.println("【queryWindow】:" + queryWindow.toString());
			RTree index = value.getIndex();
			//System.out.println(index.toString());
			RTreeNode root = index.getRoot();
			//System.out.println(root.toString());
			MBR areaMbr = root.getMbr();
			if ((areaMbr.getTopRight().getStart() >= queryWindow.getStart()) && (areaMbr.getTopRight().getEnd() <= queryWindow.getEnd())
					|| ((areaMbr.getBottomLeft().getStart() >= queryWindow.getStart()) && (areaMbr.getBottomLeft().getEnd() <= queryWindow.getEnd()))) {
				index.setDiskFileName(filename);
				context.write(new IntWritable(partitionId), index);
			}
		}

		static class ReadingRTreeIndexPatitioner extends Partitioner<IntWritable, RTree> {

			@Override
			public int getPartition(IntWritable intWritable, RTree rTree, int numPartitions) {
				return intWritable.get();
			}
		}

		static class ReadingRTreeIndexReducer extends Reducer<IntWritable, RTree, Text, NullWritable> {
			Text DiskKey = new Text();
			RTreeDiskSliceFile DiskValue = new RTreeDiskSliceFile();

			@Override
			protected void reduce(IntWritable key, Iterable<RTree> values, Context context) throws IOException, InterruptedException {
				System.out.println("********************************************************************");
				System.out.println("*                              Reducer阶段                          *");
				System.out.println("********************************************************************");
				int partitionId = key.get();
				System.out.println("partitionId:" + partitionId);
				String indexFilename = values.iterator().next().getDiskFileName();//index_partitioner_0_1009478186.seq
				System.out.println("indexFilename" + indexFilename);
				//index_partitioner_0_1009478186.seq-->disk_partitioner_0.seq;
				String[] fields = indexFilename.split("_");
				String diskFilename = "disk_partitioner_" + fields[2] + ".seq";
				Path partitionDataPath = new Path(cos.getDiskFilePath() + "/RTree_disk/" + diskFilename);
				System.out.println("diskFilePath:" + partitionDataPath.toString());


				Configuration conf = new Configuration();
				ValidTime query = new ValidTime(cos.getQueryStart(), cos.getQueryEnd());
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(partitionDataPath));
				while (values.iterator().hasNext()){
					RTree index = values.iterator().next();
					RTreeQuery(index.getRoot(), query, reader, context);
				}

			}

			private void RTreeQuery(RTreeNode node, ValidTime queryWindow, SequenceFile.Reader reader, Context context) throws IOException, InterruptedException {
				for (RTreeNode n : node.getNodeList()) {
					if (n.isLeaf()) {
						n.getMbr().isIntersect(queryWindow);
						long indexOffset = n.getOffset();
						reader.seek(indexOffset);
						reader.next(DiskKey, DiskValue);
						RTreeNode leafData = DiskValue.getLeafData();
						for (Tuple tuple : leafData.getLeafData()) {
							if (queryWindow.isContainPeriod(tuple.getVt())) {
								context.write(new Text(tuple.toString()), NullWritable.get());
							}
						}
						return;
					} else {
						n.getMbr().isIntersect(queryWindow);
						RTreeQuery(n, queryWindow, reader, context);
					}
				}
			}
		}

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			cluster_running();
			//local_running();
		}

		public static void cluster_running() throws InterruptedException, IOException, ClassNotFoundException {
			Configuration conf = new Configuration();
			conf.set("fs.default", "hdfs://192.168.69.204:8020");
			conf.set("mapreduce.framework.name", "yarn");
			conf.setBoolean("fs.hdfs.impl.disable.cache", true);
			//System.setProperty("HADOOP_USER_NAME", "root");

			conf.set("mapreduce.job.jar",CONSTANTS.getMapReduceJobJarPath());

			Job job = Job.getInstance(conf, "readRTreeIndex_cluster");


			job.setNumReduceTasks(cos.getNumOfYDimention() * cos.getNumOfXDimention());
			job.setJarByClass(ReadingRTreeIndex.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(RTree.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			//-----------------------------------------------------------------------
			job.setReducerClass(ReadingRTreeIndexReducer.class);
			job.setPartitionerClass(ReadingRTreeIndexPatitioner.class);
			job.setMapperClass(ReadingRTreeIndexMapper.class);
			SequenceFileInputFormat.addInputPath(job, new Path(cos.getDiskFilePath() + "/RTreeIndex"));
			Path outPath = new Path(cos.getDataFileDir() + "/queryInfo/");//用于mr输出success信息的路径
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}
			FileOutputFormat.setOutputPath(job, outPath);

			boolean res = job.waitForCompletion(true);
			System.exit(res ? 0 : 1);
		}

		public static void local_running() throws InterruptedException, IOException, ClassNotFoundException {
			Configuration conf = new Configuration();
			conf.set("mapreduce.framework.name", "local");
			conf.setBoolean("fs.hdfs.impl.disable.cache", true);
			//System.setProperty("HADOOP_USER_NAME", "root");
			Job job = Job.getInstance(conf, "readRTreeIndex");


			job.setNumReduceTasks(cos.getNumOfYDimention() * cos.getNumOfXDimention());
			System.out.println("Reducer number:"+cos.getNumOfYDimention() * cos.getNumOfXDimention());
			job.setJarByClass(ReadingRTreeIndex.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(RTree.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			//-----------------------------------------------------------------------
			job.setReducerClass(ReadingRTreeIndexReducer.class);
			job.setPartitionerClass(ReadingRTreeIndexPatitioner.class);
			job.setMapperClass(ReadingRTreeIndexMapper.class);
			SequenceFileInputFormat.addInputPath(job, new Path(cos.getDiskFilePath() + "/RTreeIndex/"));
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
}