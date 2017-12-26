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
    private static long RecordCount;
    static class SRSMapper extends Mapper<Object,Text,NullWritable,Text>{
        private Random rands = new Random();
        private Double percetage ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String strPercentage = "10";
            percetage = Double.parseDouble(strPercentage)/100.00;
        }

        @Override
        protected void map(Object keyIntWritable, Text value, Context context) throws IOException, InterruptedException {
            if (rands.nextDouble()<percetage){
                RecordCount++;
                context.write(NullWritable.get(),value);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            CONSTANTS.setRecord_nums(RecordCount);
            CONSTANTS.persistenceData();
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
        FileInputFormat.setInputPaths(job, "/home/think/Desktop/data/data.txt");
        Path outPath = new Path("/home/think/Desktop/data/sample");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
       // System.exit(res ? 0 : 1);
    }
}
