package cn.edu.scnu.dtindex.bigdata.seqfile;

import cn.edu.scnu.dtindex.model.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SeqFileMapper {

    private static SequenceFile.Reader reader = null;
    private static Configuration conf = new Configuration();
    public static class ReadFileMapper extends
            Mapper<Text, Tuple, Text, Text> {

        /* (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(Text key, Tuple value, Context context) throws IOException{
            key = (Text) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            value = (Tuple) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            reader.seek(302);
            try {
                while (reader.next(key, value)) {

                    System.out.printf("%s\t%s\n", key, value.toString());
                    context.write(key,new Text(value.toString()));

                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }

    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {


        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf, "seq_local");

        job.setJarByClass(SeqFileMapper.class);
        job.setMapperClass(ReadFileMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setNumReduceTasks(0);
        Path path = new Path("/Users/think/Desktop/seqTuple.seq");
        FileSystem fs = FileSystem.get(conf);
        reader = new SequenceFile.Reader(fs, path, conf);
        Path outPath = new Path("/Users/think/Desktop/result");

        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, path);
        FileOutputFormat.setOutputPath(job, outPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}