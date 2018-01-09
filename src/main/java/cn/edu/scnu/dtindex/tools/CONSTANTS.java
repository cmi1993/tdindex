package cn.edu.scnu.dtindex.tools;


import org.apache.hadoop.conf.Configuration;

import java.io.*;

public class CONSTANTS implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final String clusterAdd = "hdfs://192.168.69.204:8020";
	private static String dataScalaDir = "4000w";
	private double HADOOP_BLOCK_SIZE = 128;//hadoop磁盘块大小
	private double apha = 0.0;//索引所需空间的膨胀系数
	private double numOfPartition;//分区数量
	//private  int numOfEachdimention;//每一个维度的切分数=根号（分区数量）再取整
	private int numOfXDimention;//切分后，x轴方向的分区数
	private int numOfYDimention;//切分后，y轴方向的分区数
	private final String constants_persistence_path = clusterAdd+"/timeData/"+dataScalaDir+"/contants.dat";//常量数据持久化路径
	private long record_nums;//总记录数
	//-----------------------------------------------------------------------------------------
	private String dataFileDir = clusterAdd + "/timeData/" + dataScalaDir;//数据路径
	//-----------------------------------------------------------------------------------------
	private String dataFilePath = dataFileDir + "/data.txt";//数据文件
	private String samplerFileDir = clusterAdd + "/timeData/" + dataScalaDir + "/sampleData";//采样后样本存放路径
	private String samplerFilePath = samplerFileDir + "/sampler.txt";//采样样文件路径
	private String classifiedFilePath = dataFileDir + "/classifiedData";//数据切片存放路径
	private String XsortedDataDir = dataFileDir + "/SampleSort/XSortTmp";//x排序路径
	private String YsortedDataDir = dataFileDir + "/SampleSort/YSortTmp";//y排序路径
	private String queryInfoDir = dataFileDir+"/queryInfo";
	private long[] xPatitionsData = new long[numOfXDimention + 1];//保存x分界点
	private long[][] yPatitionsData = new long[numOfXDimention][numOfYDimention + 1];//保存y分界点
	private Double percentage = Double.parseDouble("10") / 100.00;//采样率
	private String DiskFilePath = dataFileDir+"/DiskSliceFile";//磁盘块序列化路径
	private String indexFileDir = DiskFilePath + "/index";//索引文件存放路径，查询时候会首先加载索引
	private String diskSliceFileDir = DiskFilePath + "/disk";//索引文件存放路径，查询时候会首先加载索引
	private String queryStart;
	private String queryEnd;


	//-----------------------------------单例模式--------------------------------------------
	private static class CONSTANTSHolder {
		private static final CONSTANTS INSTANCE = new CONSTANTS();
	}

	private CONSTANTS() {
	}

	public static final CONSTANTS getInstance() {
		return CONSTANTSHolder.INSTANCE;
	}
	//-----------------------------------单例模式--------------------------------------------

	/**
	 * 对象模型序列化到磁盘
	 *
	 * @param object
	 * @throws IOException
	 */
	public static void persistenceData(CONSTANTS object) throws IOException {
		HDFSTool hdfsTool = new HDFSTool(new Configuration());
		hdfsTool.objectStreamToHdfs(object,CONSTANTS.getInstance().constants_persistence_path);
	}

	/**
	 * 反序列化对象并读取数据
	 *
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static CONSTANTS readPersistenceData() throws IOException, ClassNotFoundException {
		HDFSTool hdfs = new HDFSTool(new Configuration());
		Object o = hdfs.objectFromHdfs(clusterAdd + "/timeData/"+dataScalaDir+"/contants.dat");
		CONSTANTS cos = (CONSTANTS) o;
		return cos;

	}

	/**
	 * 打印字段信息
	 */
	public void showConstantsInfo() {
		System.out.println("------------------------------------------【常量信息】------------------------------------------");
		System.out.println("|HADOOP_BLOCK_SIZE    		|hadoop磁盘块大小		|" + HADOOP_BLOCK_SIZE);
		System.out.println("|apha                 		|索引所需空间的膨胀系数	|" + apha);
		System.out.println("|numOfPartition       		|分区数量				|" + numOfPartition);
		System.out.println("|numOfXDimention   		    |X维度的切分数	    |" + numOfXDimention);
		System.out.println("|numOfYDimention   		    |Y维度的切分数	    |" + numOfYDimention);
		System.out.println("|constants_persistence_path |常量数据持久化路径    |" + constants_persistence_path);
		System.out.println("|record_nums          		|总记录数				|" + record_nums);
		System.out.println("|dataFilePath       		|数据文件				|" + dataFilePath);
		System.out.println("|dataFileDir         		|数据路径				|" + dataFileDir);
		System.out.println("|samplerFilePath            |采样样文件路径		|" + samplerFilePath);
		System.out.println("|samplerFileDir             |采样样文件路径		|" + samplerFileDir);
		System.out.println("|XsortedDataDir         	|x排序路径			|" + XsortedDataDir);
		System.out.println("|YsortedDataDir             |y排序路径		    |" + YsortedDataDir);
		System.out.println("|percentage                 |采样率		        |" + percentage);
		System.out.println("|DiskFilePath               |磁盘块序列化路径	    |" + DiskFilePath);
		System.out.println("|indexFileDir               |索引块序列化路径	    |" + indexFileDir);
		System.out.println("|diskSliceFileDir           |磁盘切片序列化路径    |" + diskSliceFileDir);
		System.out.println("|queryStart                 |查询窗口的开始时间    |" + queryStart);
		System.out.println("|queryEnd                   |查询窗口的结束时间    |" + queryEnd);
		System.out.println("----------------------------------------------------------------------------------------------");
		System.out.print("|x分区采样点\t|");
		for (long x : xPatitionsData) {
			System.out.print(x + "|");
		}
		System.out.println();
		System.out.println("----------------------------------------------------------------------------------------------");
		System.out.print("|y分区采样点\t|");
		System.out.println();
		for (int i = 0; i < yPatitionsData.length; i++) {
			System.out.print("|         \t|");
			for (int j = 0; j < yPatitionsData[i].length; j++) {
				System.out.print(yPatitionsData[i][j] + "|");
			}
			System.out.println();
		}
		System.out.println();
		System.out.println("------------------------------------------【常量信息】------------------------------------------");

	}


	public  String getDiskSliceFileDir() {
		return diskSliceFileDir;
	}

	public  void setDiskSliceFileDir(String diskSliceFileDir) {

		this.diskSliceFileDir = diskSliceFileDir;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		CONSTANTS constants = CONSTANTS.getInstance().readPersistenceData();
		constants.setQueryStart("123");
		persistenceData(constants);
		/*CONSTANTS cos = CONSTANTS.getInstance().readPersistenceData();
		cos.showConstantsInfo();*/
	}


	//-----------------------------------getter and setter----------------------------------------------------


	public String getQueryInfoDir() {
		return queryInfoDir;
	}

	public void setQueryInfoDir(String queryInfoDir) {
		this.queryInfoDir = queryInfoDir;
	}

	public String getClusterAdd() {
		return clusterAdd;
	}

	public String getDataScalaDir() {
		return dataScalaDir;
	}

	public void setDataScalaDir(String dataScalaDir) {
		this.dataScalaDir = dataScalaDir;
	}

	public double getHADOOP_BLOCK_SIZE() {
		return HADOOP_BLOCK_SIZE;
	}

	public void setHADOOP_BLOCK_SIZE(double HADOOP_BLOCK_SIZE) {
		this.HADOOP_BLOCK_SIZE = HADOOP_BLOCK_SIZE;
	}

	public double getApha() {
		return apha;
	}

	public void setApha(double apha) {
		this.apha = apha;
	}

	public double getNumOfPartition() {
		return numOfPartition;
	}

	public void setNumOfPartition(double numOfPartition) {
		this.numOfPartition = numOfPartition;
	}

	public int getNumOfXDimention() {
		return numOfXDimention;
	}

	public void setNumOfXDimention(int numOfXDimention) {
		this.numOfXDimention = numOfXDimention;
	}

	public int getNumOfYDimention() {
		return numOfYDimention;
	}

	public void setNumOfYDimention(int numOfYDimention) {
		this.numOfYDimention = numOfYDimention;
	}

	public String getConstants_persistence_path() {
		return constants_persistence_path;
	}

	public long getRecord_nums() {
		return record_nums;
	}

	public void setRecord_nums(long record_nums) {
		this.record_nums = record_nums;
	}

	public String getDataFileDir() {
		return dataFileDir;
	}

	public void setDataFileDir(String dataFileDir) {
		this.dataFileDir = dataFileDir;
	}

	public String getDataFilePath() {
		return dataFilePath;
	}

	public void setDataFilePath(String dataFilePath) {
		this.dataFilePath = dataFilePath;
	}

	public String getSamplerFileDir() {
		return samplerFileDir;
	}

	public void setSamplerFileDir(String samplerFileDir) {
		this.samplerFileDir = samplerFileDir;
	}

	public String getSamplerFilePath() {
		return samplerFilePath;
	}

	public void setSamplerFilePath(String samplerFilePath) {
		this.samplerFilePath = samplerFilePath;
	}

	public String getClassifiedFilePath() {
		return classifiedFilePath;
	}

	public void setClassifiedFilePath(String classifiedFilePath) {
		this.classifiedFilePath = classifiedFilePath;
	}

	public String getXsortedDataDir() {
		return XsortedDataDir;
	}

	public void setXsortedDataDir(String xsortedDataDir) {
		XsortedDataDir = xsortedDataDir;
	}

	public String getYsortedDataDir() {
		return YsortedDataDir;
	}

	public void setYsortedDataDir(String ysortedDataDir) {
		YsortedDataDir = ysortedDataDir;
	}

	public long[] getxPatitionsData() {
		return xPatitionsData;
	}

	public void setxPatitionsData(long[] xPatitionsData) {
		this.xPatitionsData = xPatitionsData;
	}

	public long[][] getyPatitionsData() {
		return yPatitionsData;
	}

	public void setyPatitionsData(long[][] yPatitionsData) {
		this.yPatitionsData = yPatitionsData;
	}

	public Double getPercentage() {
		return percentage;
	}

	public void setPercentage(Double percentage) {
		this.percentage = percentage;
	}

	public String getDiskFilePath() {
		return DiskFilePath;
	}

	public void setDiskFilePath(String diskFilePath) {
		DiskFilePath = diskFilePath;
	}

	public String getIndexFileDir() {
		return indexFileDir;
	}

	public void setIndexFileDir(String indexFileDir) {
		this.indexFileDir = indexFileDir;
	}

	public String getQueryStart() {
		return queryStart;
	}

	public void setQueryStart(String queryStart) {
		this.queryStart = queryStart;
	}

	public String getQueryEnd() {
		return queryEnd;
	}

	public void setQueryEnd(String queryEnd) {
		this.queryEnd = queryEnd;
	}
}