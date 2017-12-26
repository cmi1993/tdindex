package cn.edu.scnu.dtindex.bigdata.wcDemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * KEYIN,VALUEIN对应mapper输出的keyout，valueout
 * 
 * keyout，valueout是自定义reduce逻辑处理结果的输出数据类型
 * keyout是单词
 * valueout是总次数
 * @author think
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int count = 0;//定义一个计数器
		for (IntWritable value : values) {
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	}
}
