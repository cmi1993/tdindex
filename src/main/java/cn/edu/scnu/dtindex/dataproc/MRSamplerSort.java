package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import cn.edu.scnu.dtindex.tools.HDFSTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MRSamplerSort {
	static CONSTANTS cos = CONSTANTS.getInstance();

	public static void CalcPartitions() throws IOException {
		//3.计算分区数量
		long sampleRecordCount = CONSTANTS.getDatanum() / 10;
		cos.setSampleRecord_nums(sampleRecordCount);
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

	}


	static class SortXMapper extends Mapper<LongWritable, Text, Tuple, NullWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			Tuple tuple = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
			context.write(tuple, NullWritable.get());
		}
	}

	static class SortYReducer extends Reducer<Tuple, NullWritable, Text, NullWritable> {

		@Override
		protected void reduce(Tuple key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {


			context.write(new Text(key.toString()), NullWritable.get());
		}
	}


	public static void SplitDataToMultiparts(int numsOfSlice) {

	}

	public static void main(String[] args) throws IOException {
		CalcPartitions();
	}

}
