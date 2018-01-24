package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountMapper;
import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountReducer;
import cn.edu.scnu.dtindex.bigdata.wcDemo.WordCountRunner;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.Reader;
import java.util.Random;

public class Sampler {
	static CONSTANTS cos;

	static {
			cos = CONSTANTS.getInstance();
	}
    private static long RecordCount;
    static class SRSMapper extends Mapper<Object,Text,NullWritable,Text>{
        private Random rands = new Random();
        private Double percetage ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String strPercentage = "5";
            percetage = Double.parseDouble(strPercentage)/100.00;
        }

        @Override
        protected void map(Object keyIntWritable, Text value, Context context) throws IOException, InterruptedException {
            if (rands.nextDouble()<percetage){
                RecordCount++;
                context.write(NullWritable.get(),value);
            }
        }

    }
    static class SamplerOutPutFormat extends FileOutputFormat<NullWritable,Text>{

        @Override
        public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            FileSystem fs =FileSystem.get(context.getConfiguration());
            Path sampleOutPath = new Path(cos.getSamplerFilePath());
            FSDataOutputStream samplerOut = fs.create(sampleOutPath);
            return new MyRecordWriter(samplerOut);
        }
    }
    static class MyRecordWriter extends RecordWriter<NullWritable,Text>{
        FSDataOutputStream samplerOut = null;

        public MyRecordWriter(FSDataOutputStream samplerOut) {
            this.samplerOut = samplerOut;
        }


        @Override
        public void write(NullWritable key, Text value) throws IOException, InterruptedException {
            samplerOut.write((value.toString()+"\n").getBytes());
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            if (samplerOut!=null)
                samplerOut.close();
        }
    }

   static class SamplePatitioner extends Partitioner<NullWritable,Text>{

        @Override
        public int getPartition(NullWritable nullWritable, Text text, int i) {
            return 0;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://192.168.69.204:8020");
		conf.set("mapreduce.framework.name","yarn");

		conf.set("mapreduce.job.jar", "/Users/think/Library/Mobile Documents/com~apple~CloudDocs/tdindex/target/dtindex-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Job job = Job.getInstance(conf, "sampler_cluster_runung");
		job.setJarByClass(Sampler.class);
		job.setMapperClass(SRSMapper.class);
		job.setNumReduceTasks(1);
		job.setPartitionerClass(SamplePatitioner.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SamplerOutPutFormat.class);
		FileInputFormat.setInputPaths(job,cos.getDataFilePath());
		Path outPath = new Path(cos.getSamplerFileDir());
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

	private static void local() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
		conf.set("mapreduce.framework.name", "local");
		Job job = Job.getInstance(conf, "sampler_local");


		job.setJarByClass(Sampler.class);
		job.setMapperClass(SRSMapper.class);
		job.setNumReduceTasks(1);
		job.setPartitionerClass(SamplePatitioner.class);


		// 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(SamplerOutPutFormat.class);
		FileInputFormat.setInputPaths(job, cos.getDataFilePath());
		Path outPath = new Path(cos.getSamplerFileDir());
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
