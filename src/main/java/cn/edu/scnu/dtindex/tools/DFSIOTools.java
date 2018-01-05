package cn.edu.scnu.dtindex.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.net.URI;

public class DFSIOTools {
	public static void main(String[] args) throws IOException {
		/*toWrite(new Configuration(),"12345","/test/2.txt",0);
		toWrite(new Configuration(),"line4,in,the,line4","/test/2.txt",1);
		System.out.println("success");
		*/

		String s = toReadWithCharReturn(new Configuration(), "hdfs://master:8020/timeData/1000w/classifiedData/small.txt");
		String[] split = s.split("\n");
		System.out.println();
		//System.out.println(toRead(new Configuration(), "hdfs://192.168.69.204:8020/test/1.txt"));
	}

	/**
	 * 写出数据(一次性写入)
	 *
	 * @param str--要写出的数据(字符串)
	 * @param path--写出的路径
	 * @throws IOException
	 */
	public static void toWrite(Configuration conf, String str, String path,int flag) throws IOException {
		if (flag==0) {
			HDFSTool tool = new HDFSTool(conf);
			tool.createFile(path, str);
		}else {
			HDFSTool tool =new HDFSTool(conf);
			tool.append(path,str);

		}

	}

	/**
	 * 按行读取数据
	 *
	 * @param path--要读取的数据的路径
	 * @return 读出来的数据，字符串，以空格分隔
	 * @throws IOException
	 */
	public static String toRead(Configuration conf, String path) throws IOException {

		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			fsr = fs.open(new Path(path));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				buffer.append(lineTxt).append(" ");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return buffer.toString();

	}

	public static String toReadWithCharReturn(Configuration conf, String path) throws IOException {

		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			fsr = fs.open(new Path(path));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				buffer.append(lineTxt).append("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return buffer.toString();

	}


	public static String toReadWithSpecialSplitSignal(Configuration conf , String path) throws IOException {
		StringBuffer buffer = new StringBuffer();
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;
		String lineTxt = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			fsr = fs.open(new Path(path));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while ((lineTxt = bufferedReader.readLine()) != null) {
				buffer.append(lineTxt).append("#");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (bufferedReader != null) {
				try {
					bufferedReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return buffer.toString();

	}

}
