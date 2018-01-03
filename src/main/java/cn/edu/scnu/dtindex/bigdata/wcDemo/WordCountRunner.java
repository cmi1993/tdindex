package cn.edu.scnu.dtindex.bigdata.wcDemo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 相当于一个yarn集群的客户端 需要在次封装我们的mr程序的相关运行参数，指定jar包 最后交给yarn
 * 
 * @author think
 *
 */
public class WordCountRunner {
	// 把业务逻辑相关信息（那个是mapper，哪个是reducer，要处理的数据在哪里，输出的结果放哪里...）描述成一个job对象
	// 把这个描述好的job提交给集群去运行
	public static void main(String[] args) throws Exception {
		local_runing();//本地模式
		//cluster_runung();//集群模式
	}

	private static void cluster_runung() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://master:8020");
	    conf.set("mapreduce.framework.name","yarn"); 
		//*//**//**//**//**//**//**//**//**
		 //* 需指定文件系统，否则
		 //* hadoop程序抛出异常：java.lang.IllegalArgumentException: Wrong FS: hdfs:/ expected file:///
		 //*//**//**//**//**//**//**//**//*/
		Job job = Job.getInstance(conf, "wordcount_cluster_runung");
		job.setJar("/Users/think/Documents/hadoop_study_tmp/dtindex.jar");
		job.setJarByClass(WordCountRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 【设置我们的业务逻辑Reducer类输出的key和value的数据类型】
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, "hdfs://master:8020/test/big.txt");
		Path outPath = new Path("hdfs://master:8020/test//wordcount_out");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// 向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
		
	}

	private static void local_runing() throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "wordcount_local");

		job.setJarByClass(WordCountRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 【设置我们的业务逻辑Reducer类输出的key和value的数据类型】
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, "/Users/think/Desktop/big.txt");
		Path outPath = new Path("/Users/think/Desktop/wordcount_out");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);

		// 向yarn集群提交这个job
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
