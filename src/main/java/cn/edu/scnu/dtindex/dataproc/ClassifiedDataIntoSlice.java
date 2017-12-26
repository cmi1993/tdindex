package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;

public class ClassifiedDataIntoSlice {
	static CONSTANTS cos = CONSTANTS.getInstance();
	static class ClassifiedMapper extends Mapper<LongWritable,Text,Tuple,NullWritable>{

	}
	static class DataPatitioner extends Partitioner<Tuple,NullWritable>{
		static long[] xparts = cos.getxPatitionsData();
		static long[][] yparts = cos.getyPatitionsData();
		@Override
		public int getPartition(Tuple tuple, NullWritable nullWritable, int i) {
			int xpartOrder = SeekXPatition(tuple.getVt().getStart());
			if (xpartOrder==-1) System.out.println("查找x分区出错");
			int ypartOrder = SeekYPatition(xpartOrder,tuple.getVt().getEnd());
			return 0;
		}

		private int SeekYPatition(int xpartOrder,long end) {

			return -1;//找不到
		}

		private int SeekXPatition(long start) {
			if (start<=MidValue(xparts[1],xparts[2]))return 0;
			else if (start>=MidValue(xparts[xparts.length-2],xparts[xparts.length-3]))return xparts.length/2-1;
			else {
				for (int i = 1; i <= xparts.length-5; i=i+2) {
					if (start>=MidValue(xparts[i],xparts[i+1])&&start<=MidValue(xparts[i+3],xparts[i+4]))return (i+1)/2;
				}
			}
			return -1;//找不到
		}

		private long MidValue(long num1, long num2) {
			return (num1+num2)/2;
		}
	}
}
