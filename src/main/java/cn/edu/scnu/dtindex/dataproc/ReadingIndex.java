package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.DiskSliceFile;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class ReadingIndex {

	static CONSTANTS cos;
	static {
		try {
			cos = CONSTANTS.readPersistenceData();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	static class ReadingIndexMapper extends Mapper<Text,DiskSliceFile,Text,Text>{
		@Override
		protected void map(Text key, DiskSliceFile value, Context context) throws IOException, InterruptedException {
			System.out.println("key:"+key.toString());
			System.out.println("value:"+value.toString());
			System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {




		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "readIndex_local");


		job.setJarByClass(ReadingIndex.class);

		job.setNumReduceTasks(0);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ReadingIndexMapper.class);
		SequenceFileInputFormat.addInputPath(job,new Path(CONSTANTS.getIndexFileDir()+"/index_small.txt.seq"));
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
