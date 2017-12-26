package cn.edu.scnu.dtindex.bigdata.wcDemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * KEYIN：默认情况下，是mr框架所读到的一行文本的起始偏移量，Long
 * 但是在hadoop中有自己更加精简的序列化接口，所以不直接使用Long，而用LongWritable「org.apache.hadoop.io」
 * VALUEIN:默认情况下，是mr框架所读到的一行文本的内容，String，同上，使用Text【org.apache.hadoop.io】
 * 
 * <单词，次数>
 * KEYOUT：是用户自定义逻辑处理完成后输出数据的key，在这里是单词，String
 * VLAUEOUT：是用户自定义逻辑处理完成后输出数据的value，在这里是单词次数，Integer，用IntWritable「org.apache.hadoop.io」
 * @author think
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	/**
	 * map阶段的业务逻辑就写在自定义的map（）方法中，maptask会对每一行输入的数据调用一次我们自定义的map方法
	 */
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();//拿到一行数据转化为String
		String[] words = line.split(" ");//切分出这一行的单词
		for (String word : words) {//遍历数组，输出<单词,1>
			context.write(new Text(word), new IntWritable(1));
		}
	}
}
