package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class _tmpSamplerSort {
    static List<String> yClassifiedPath;//y排序的分区路径表
    static int count =0;

    static class SamplerData_XSort extends Mapper<LongWritable,Text,Tuple,NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line  = value.toString();
            String[] splits = line.split(",");
            Tuple t = new Tuple(splits[0],splits[1],splits[2],splits[3],splits[4],splits[5]);
            count++;
            context.write(t,NullWritable.get());

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("count:"+count);
            //3.计算分区数量
            File file = new File("/home/think/Desktop/data/data.txt");
            long length = (file.length()/1024/1024);
            double numOfPartition = length*(1+ CONSTANTS.getApha())/CONSTANTS.getHadoopBlockSize();//分区数量
            long numOfEachdimention = Math.round(Math.sqrt(numOfPartition));//每个维度的分割数两
            CONSTANTS.setNumOfPartition(numOfPartition);
            CONSTANTS.setNumOfEachdimention((int)numOfEachdimention);
            long perXpartNum = CONSTANTS.getRecord_nums()/CONSTANTS.getNumOfEachdimention();//x维度每部分的切割数量
            CONSTANTS.setRecord_nums(count);
            CONSTANTS.persistenceData();
            //生成所有分区路径
            yClassifiedPath = new ArrayList<String>();
            for (int i = 0; i < CONSTANTS.getNumOfEachdimention(); i++) {
                for (int j = 0; j < CONSTANTS.getNumOfEachdimention(); j++) {
                    yClassifiedPath.add("/home/think/Desktop/data/SampleSort/Sort_part_"+i+"_"+j+"");
                }
            }
            System.out.println("Mapper 结束，参数初始化成功");
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        System.setProperty("hadoop.home.dir", "/home/think/app/hadoop-2.6.0");
        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "sampler_x_sort_local");


        job.setJarByClass(_tmpSamplerSort.class);
        job.setMapperClass(SamplerData_XSort.class);

        job.setNumReduceTasks(1);
        // 【设置我们的业务逻辑Mapper类输出的key和value的数据类型】
        job.setMapOutputKeyClass(Tuple.class);
        job.setMapOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, "/home/think/Desktop/data/sample/part-r-00000");
        Path outPath = new Path("/home/think/Desktop/data/sort_x_result");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        // 向yarn集群提交这个job
        boolean res = job.waitForCompletion(true);
        //System.exit(res ? 0 : 1);
    }
}
