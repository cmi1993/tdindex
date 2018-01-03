package cn.edu.scnu.dtindex.tools;


import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.HsftpFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

/**
 * HDFS工具类
 * <p>
 * 实现功能：
 * hadoop fs -ls /
 * hadoop fs -mkdir /data
 * hadoop fs -rmr /data/test.txt
 * hadoop fs -copyFromLocal /test/test.txt /data
 * hadoop fs -cat /data/test.txt
 * hadoop fs -copyToLocal /data /test/test.txt
 * 创建一个新文件，并写入内容
 * 重命名
 * <p>
 * 需要导入以下路径的所有jar包： hadoop-2.7.2\share\hadoop\common
 * hadoop-2.7.2\share\hadoop\common\lib hadoop-2.7.2\share\hadoop\hdfs
 * hadoop-2.7.2\share\hadoop\hdfs\lib hadoop-2.7.2\share\hadoop\mapreduce
 *
 * @author Skye
 */
public class HDFSTool {

	// HDFS访问地址
	private static final String HDFS = CONSTANTS.getClusterAdd();
	// hdfs路径
	private String hdfsPath;
	// Hadoop系统配置
	private Configuration conf;

	public HDFSTool(Configuration conf) {
		this(HDFS, conf);
	}

	public HDFSTool(String hdfs, Configuration conf) {
		this.hdfsPath = hdfs;
		this.conf = conf;
	}

	// 启动函数
	public static void main(String[] args) throws IOException {
		JobConf conf = config();
		HDFSTool hdfs = new HDFSTool(conf);
		System.out.println(hdfs.isExits("/timeData/1000w/data.txt"));
		System.out.println(hdfs.isExits("/timeData/1000w/data1.txt"));
		//hdfs.ls("/");
		// hdfs.mkdirs("/new");
		// 可以同时建多级目录
		// hdfs.mkdirs("/new/new3");
		// hdfs.ls("/tuijian");
		// hdfs.rmr("/new");
		// 可用当前eclipse工作空间的相对路径和文件绝对路径 以及当前项目的路径不加"/"
		// hdfs.copyFileToHdfs("data/hive笔记.md", "/data");
		// hdfs.copyFileToHdfs("/Xiaomi/MiFlashClean.cmd", "/data");
		// hdfs.copyFileToHdfs("E:/推荐系统/100万用户数据/user_pay", "/data");
		// hdfs.rmr("/data/MiFlashClean.cmd");
		// hdfs.rmr("/data/user_pay_201606");
		// hdfs.createFile("/new/createTest", "1,英雄联盟");
		// hdfs.download("/data/RecommendList", "C:/Users/Skye/Desktop");
		// hdfs.cat("/data/RecommendList1");
		// hdfs.renameFile("/data/RecommendList", "/data/RecommendListOld");
		// hdfs.ls("/data");
		//hdfs.findLocationOnHadoop("/data/RecommendListOld");
	}

	// 加载Hadoop配置文件
	public static JobConf config() {
		JobConf conf = new JobConf(HDFSTool.class);
		conf.setJobName("HDFSTool");
		// conf.addResource("classpath:/hadoop/core-site.xml");
		// conf.addResource("classpath:/hadoop/hdfs-site.xml");
		// conf.addResource("classpath:/hadoop/mapred-site.xml");
		return conf;
	}

	public void mkdirs(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if (!fs.exists(path)) {
			fs.mkdirs(path);
			System.out.println("Create: " + folder);
		}
		fs.close();
	}

	public void rmr(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.deleteOnExit(path);
		System.out.println("Delete: " + folder);
		fs.close();
	}

	public void ls(String folder) throws IOException {
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FileStatus[] list = fs.listStatus(path);
		System.out.println("ls: " + folder);
		System.out.println("==========================================================");
		for (FileStatus f : list) {
			System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
		}
		System.out.println("==========================================================");
		fs.close();
	}

	public void createFile(String file, String content) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		byte[] buff = content.getBytes();
		FSDataOutputStream os = null;
		try {
			os = fs.create(new Path(file));
			os.write(buff, 0, buff.length);
			System.out.println("Create: " + file);
		} finally {
			if (os != null)
				os.close();
		}
		fs.close();
	}

	public void copyFileToHdfs(String local, String remote) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyFromLocalFile(new Path(local), new Path(remote));
		System.out.println("copy from: " + local + " to " + remote);
		fs.close();
	}

	public void download(String remote, String local) throws IOException {
		Path path = new Path(remote);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		fs.copyToLocalFile(path, new Path(local));
		System.out.println("download: from" + remote + " to " + local);
		fs.close();
	}

	public void renameFile(String oldFileName, String newFileName) throws IOException {
		boolean isSuccess = true;

		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		try {
			isSuccess = fs.rename(new Path(oldFileName), new Path(newFileName));
		} catch (IOException e) {
			isSuccess = false;
		}
		System.out.println(isSuccess ? "Rename success！ " + oldFileName + " to " + newFileName
				: "Rename failed！" + oldFileName + " to " + newFileName);
		fs.close();
	}

	/**
	 * 查看某个文件在HDFS集群的位置
	 *
	 * @throws IOException
	 */

	public void findLocationOnHadoop(String filePath) throws IOException {
		// Path targetFile=new Path(rootPath+"user/hdfsupload/AA.txt");
		// FileStatus fileStaus=coreSys.getFileStatus(targetFile);
		Path targetFile = new Path(HDFS + filePath);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FileStatus fileStaus = fs.getFileStatus(targetFile);
		BlockLocation[] bloLocations = fs.getFileBlockLocations(fileStaus, 0, fileStaus.getLen());
		for (int i = 0; i < bloLocations.length; i++) {
			System.out.println("block_" + i + "_location:" + bloLocations[i].getHosts()[0]);
		}
		fs.close();
	}

	public void cat(String remoteFile) throws IOException {
		Path path = new Path(remoteFile);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		FSDataInputStream fsdis = null;
		System.out.println("cat: " + remoteFile);
		try {
			fsdis = fs.open(path);
			IOUtils.copyBytes(fsdis, System.out, 4096, false);
		} finally {
			IOUtils.closeStream(fsdis);
			fs.close();
		}
	}

	public boolean isExits(String filePath) throws IOException {
		Path path = new Path(filePath);
		FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		if (!fs.exists(path)) {
			fs.close();
			return false;
		} else {
			fs.close();
			return true;
		}


	}

}
