package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import cn.edu.scnu.dtindex.tools.DFSIOTools;
import cn.edu.scnu.dtindex.tools.HDFSTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SamplerSort {
	static CONSTANTS cos;
	static List<String> xClassifiedPath;
	static List<String> yClassifiedPath;

	static class NoneOpMapper extends Mapper<NullWritable, NullWritable, NullWritable, NullWritable> {
		@Override
		protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
			super.map(key, value, context);
		}
	}

	public static void sortX(String Rpath) throws IOException {
		String str = DFSIOTools.toReadWithSpecialSplitSignal(new Configuration(), Rpath);
		String[] records = str.split("#");
		List<Tuple> tuplesList = new ArrayList<Tuple>();
		long record_count = 0;
		for (String record : records) {
			String[] fields = record.split(",");
			Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
			tuplesList.add(t);
			record_count++;
		}
		Collections.sort(tuplesList);
		//3.计算分区数量
		cos.setSampleRecord_nums(record_count);
		HDFSTool hdfs = new HDFSTool(new Configuration());
		long length = hdfs.getFileLength(cos.getDataFilePath()) / 1024 / 1024;
		double numOfPartition = length * (1 + cos.getApha()) / cos.getHADOOP_BLOCK_SIZE();//分区数量
		long numOfEachdimention = Math.round(Math.sqrt(numOfPartition));//每个维度的分割数两
		cos.setNumOfPartition(numOfPartition);

		/*if(numOfEachdimention*numOfEachdimention<numOfPartition){//如果需要拉伸分区
			cos.setApha(0);//拉伸分区就不需要进行膨胀系数的考虑
			numOfPartition = length * (1 + cos.getApha()) / cos.getHadoopBlockSize();//分区数量
			numOfEachdimention = Math.round(Math.sqrt(numOfPartition));//每个维度的分割数两
			cos.setNumOfPartition(((int)numOfEachdimention+1)*((int)numOfEachdimention));
			cos.setNumOfXDimention((int)numOfEachdimention+1);
			cos.setNumOfYDimention((int)numOfEachdimention);
			
		}else {
			cos.setNumOfXDimention((int)numOfEachdimention);
			cos.setNumOfYDimention((int)numOfEachdimention);
		}*/
		//不拉伸分区实验----------------------------------------
		cos.setNumOfXDimention((int) numOfEachdimention);
		cos.setNumOfYDimention((int) numOfEachdimention);

		cos.setNumOfPartition(cos.getNumOfYDimention() * cos.getNumOfXDimention());
		//不拉伸分区实验----------------------------------------
		long perXpartNum = cos.getSampleRecord_nums() / cos.getNumOfXDimention();//x维度每部分的切割数量

		long[] xparts = new long[cos.getNumOfXDimention() + 1];//保存x分界点
		//生成x排序临时文件分区路径
		xClassifiedPath = new ArrayList<String>();
		for (int i = 0; i < cos.getNumOfXDimention(); i++) {
			xClassifiedPath.add(cos.getXsortedDataDir() + "/x_part_" + i);
		}
		//生成所有分区路径
		yClassifiedPath = new ArrayList<String>();
		for (int i = 0; i < cos.getNumOfXDimention(); i++) {
			for (int j = 0; j < cos.getNumOfYDimention(); j++) {
				yClassifiedPath.add(cos.getYsortedDataDir() + "/Sort_part_" + i + "_" + j + "");
			}
		}
		StringBuilder stringBuilder = new StringBuilder();
		int j = 0;//用于获取分类路径的索引
		int k = 1;//用于记录写入x分区分界点数组的索引
		for (int i = 0; i < tuplesList.size(); i++) {
			if (i == 0) xparts[0] = tuplesList.get(0).getVt().getStart();
			if (i == tuplesList.size() - 1)
				xparts[xparts.length - 1] = tuplesList.get(tuplesList.size() - 1).getVt().getStart();

			stringBuilder.append(tuplesList.get(i).toString()).append("\n");
			if (i != 0 && i % (perXpartNum) == (perXpartNum - 1)) {
				if (k < xparts.length - 1) {
					long prior = tuplesList.get(i).getVt().getStart();//上一个分区的结束
					long next = tuplesList.get(i + 1).getVt().getStart();//下一个分区的开始
					xparts[k++] = MidValue(prior, next);
				}
				DFSIOTools.toWrite(new Configuration(), stringBuilder.toString(), xClassifiedPath.get(j++), 0);
				stringBuilder = new StringBuilder();
			}
		}

		DFSIOTools.toWrite(new Configuration(), stringBuilder.toString(), xClassifiedPath.get(cos.getNumOfXDimention() - 1), 1);
		cos.setxPatitionsData(xparts);
	}

	public static void sortY(String path) throws IOException {
		HDFSTool hdfsTool = new HDFSTool(new Configuration());
		FileStatus[] fileStatuses = hdfsTool.listFiles(cos.getXsortedDataDir());

		long[][] yparts = new long[cos.getNumOfXDimention()][cos.getNumOfYDimention() + 1];//保存y分界点
		for (FileStatus file : fileStatuses) {
			int xpartNum = Integer.parseInt(file.getPath().toString().substring(file.getPath().toString().length() - 1, file.getPath().toString().length()));
			String strs = DFSIOTools.toReadWithSpecialSplitSignal(new Configuration(), file.getPath().toString());
			String[] records = strs.split("#");
			List<Tuple> tupleList = new ArrayList<Tuple>();
			long record_count = 0;
			for (String record : records) {
				String[] fields = record.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
				record_count++;
			}
			long perYpartCount = record_count / cos.getNumOfYDimention();//y排序每部分的数量
			Collections.sort(tupleList, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					if (o1.getVt().getEnd() > o2.getVt().getEnd()) return 1;
					else if (o1.getVt().getEnd() < o2.getVt().getEnd()) return -1;
					else return 0;

				}
			});
			StringBuilder stringBuilder = new StringBuilder();
			int j = 0;//用于获取y分区写出路径的索引
			int k = 1;//用于记录写入x分区分界点数组的索引

			for (int i = 0; i < tupleList.size(); i++) {
				if (i == 0) yparts[xpartNum][0] = tupleList.get(0).getVt().getEnd();
				if (i == tupleList.size() - 1)
					yparts[xpartNum][cos.getNumOfYDimention()] = tupleList.get(tupleList.size() - 1).getVt().getEnd();
				stringBuilder.append(tupleList.get(i).toString()).append("\n");
				if (i != 0 && i % (perYpartCount) == (perYpartCount - 1)) {
					if (k < yparts[xpartNum].length - 1) {
						long prior = tupleList.get(i).getVt().getEnd();
						long next = tupleList.get(i + 1).getVt().getEnd();
						yparts[xpartNum][k++] = MidValue(prior, next);
					}
					DFSIOTools.toWrite(new Configuration(), stringBuilder.toString(), yClassifiedPath.get(xpartNum * cos.getNumOfYDimention() + j), 0);
					stringBuilder = new StringBuilder();
					j++;
				}
			}

			DFSIOTools.toWrite(new Configuration(), stringBuilder.toString(), yClassifiedPath.get(xpartNum * cos.getNumOfYDimention() + cos.getNumOfYDimention() - 1), 1);
		}
		cos.setyPatitionsData(yparts);
	}

	private static long MidValue(long num1, long num2) {
		return (num1 + num2) / 2;
	}


	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
		startJob();
	}
	public static String startJob() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance();
		job.setJobName("data generate");
		job.setMapperClass(NoneOpMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		/***************************
		 *
		 *......
		 *在这里，和普通的MapReduce一样，设置各种需要的东西
		 *......
		 ***************************/

		//下面为了远程提交添加设置：
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("fs.default", "hdfs://master:8020");
		conf.set("mapreduce.job.jar", "/home/think/idea project/dtindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");

		cos = CONSTANTS.getInstance();
		String pathToRead = cos.getSamplerFilePath();
		System.out.println("x排序...");
		sortX(pathToRead);

		System.out.println("x排序成功...");

		pathToRead = cos.getXsortedDataDir();
		System.out.println("y排序...");
		sortY(pathToRead);
		System.out.println("y排序成功...");

		cos.persistenceData(cos);
		cos.showConstantsInfo();
		FileInputFormat.setInputPaths(job,"/null");
		Path outPath = new Path("/generateData_info/");//用于mr输出success信息的路径
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		job.submit();
		//提交以后，可以拿到JobID。根据这个JobID可以打开网页查看执行进度。
		return job.getJobID().toString();
	}
}
