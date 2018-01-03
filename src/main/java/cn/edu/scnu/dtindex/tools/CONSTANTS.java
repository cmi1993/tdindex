package cn.edu.scnu.dtindex.tools;


import java.io.*;

public class CONSTANTS implements Serializable {
	private static final long serialVersionUID = 1L;
	private static final String clusterAdd ="hdfs://192.168.69.204:8020";
	private static String dataScalaDir = "1000w";
	private static double HADOOP_BLOCK_SIZE = 128;//hadoop磁盘块大小
	private static double apha = 0.0;//索引所需空间的膨胀系数
	private double numOfPartition;//分区数量
	//private  int numOfEachdimention;//每一个维度的切分数=根号（分区数量）再取整
	private int numOfXDimention;//切分后，x轴方向的分区数
	private int numOfYDimention;//切分后，y轴方向的分区数
	private static final String constants_persistence_path = "/home/think/Desktop/data/contants.dat";//常量数据持久化路径
	private long record_nums;//总记录数
	private static String dataFileDir = clusterAdd+"/timeData/"+dataScalaDir;//数据路径
	private static String dataFilePath = dataFileDir+"/data.txt";//数据文件
	private static String samplerFileDir = clusterAdd+"/timeData/"+dataScalaDir+"/sampleData";//采样后样本存放路径
	private static String samplerFilePath = samplerFileDir+"/sampler.txt";//采样样文件路径
	private static String classifiedFilePath = "/home/think/Desktop/data/classifiedData";//数据切片存放路径
	private static String XsortedDataDir = CONSTANTS.getDataFileDir() + "/SampleSort/XSortTmp";//x排序路径
	private static String YsortedDataDir = CONSTANTS.getDataFileDir() + "/SampleSort/YSortTmp";//y排序路径
	private long[] xPatitionsData = new long[numOfXDimention + 1];//保存x分界点
	private long[][] yPatitionsData = new long[numOfXDimention][numOfYDimention + 1];//保存y分界点
	private Double percentage = Double.parseDouble("10") / 100.00;//采样率
	private static String DiskFilePath = "/home/think/Desktop/data/DiskSliceFile";//磁盘块序列化路径
	private static String indexFileDir = CONSTANTS.getDiskFilePath() + "/index";//索引文件存放路径，查询时候会首先加载索引
	private static String diskSliceFileDir = CONSTANTS.getDiskFilePath() + "/disk";//索引文件存放路径，查询时候会首先加载索引
	private static String queryStart;
	private static String queryEnd;


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
		File file = new File(constants_persistence_path);
		if (!file.exists()) {
			file.createNewFile();
		} else {
			file.delete();
			file.createNewFile();
		}
		FileOutputStream fos = new FileOutputStream(file);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(object);
		oos.flush();
		oos.close();
		fos.close();
	}

	/**
	 * 反序列化对象并读取数据
	 *
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static CONSTANTS readPersistenceData() throws IOException, ClassNotFoundException {
		File file = new File(constants_persistence_path);
		if (!file.exists()) {
			System.out.println("持久化文件不存在");
			file.createNewFile();
		}
		FileInputStream fis = new FileInputStream(file);
		ObjectInputStream ois = new ObjectInputStream(fis);
		CONSTANTS cos = (CONSTANTS) ois.readObject();
		ois.close();
		fis.close();
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


	public static String getDiskSliceFileDir() {
		return diskSliceFileDir;
	}

	public static void setDiskSliceFileDir(String diskSliceFileDir) {
		CONSTANTS.diskSliceFileDir = diskSliceFileDir;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		CONSTANTS cos = CONSTANTS.readPersistenceData();
		cos.showConstantsInfo();
	}


	//-----------------------------------getter and setter----------------------------------------------------


	public static String getClusterAdd() {
		return clusterAdd;
	}

	public static String getQueryStart() {
		return queryStart;
	}

	public static void setQueryStart(String queryStart) {
		CONSTANTS.queryStart = queryStart;
	}

	public static String getQueryEnd() {
		return queryEnd;
	}

	public static void setQueryEnd(String queryEnd) {
		CONSTANTS.queryEnd = queryEnd;
	}

	public static String getIndexFileDir() {
		return indexFileDir;
	}

	public static void setIndexFileDir(String indexFileDir) {
		CONSTANTS.indexFileDir = indexFileDir;
	}

	public static String getClassifiedFilePath() {
		return classifiedFilePath;
	}

	public static String getDiskFilePath() {
		return DiskFilePath;
	}

	public static void setDiskFilePath(String diskFilePath) {
		DiskFilePath = diskFilePath;
	}

	public static void setClassifiedFilePath(String classifiedFilePath) {
		CONSTANTS.classifiedFilePath = classifiedFilePath;
	}

	public static String getSamplerFileDir() {
		return samplerFileDir;
	}

	public static void setSamplerFileDir(String samplerFileDir) {
		CONSTANTS.samplerFileDir = samplerFileDir;
	}

	public static double getHadoopBlockSize() {
		return HADOOP_BLOCK_SIZE;
	}

	public static void setHadoopBlockSize(double hadoopBlockSize) {
		HADOOP_BLOCK_SIZE = hadoopBlockSize;
	}

	public static double getApha() {
		return apha;
	}

	public static void setApha(double apha) {
		CONSTANTS.apha = apha;
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

	public static String getConstants_persistence_path() {
		return constants_persistence_path;
	}

	public long getRecord_nums() {
		return record_nums;
	}

	public void setRecord_nums(long record_nums) {
		this.record_nums = record_nums;
	}

	public static String getDataFilePath() {
		return dataFilePath;
	}

	public static void setDataFilePath(String dataFilePath) {
		CONSTANTS.dataFilePath = dataFilePath;
	}

	public static String getDataFileDir() {
		return dataFileDir;
	}

	public static void setDataFileDir(String dataFileDir) {
		CONSTANTS.dataFileDir = dataFileDir;
	}

	public static String getSamplerFilePath() {
		return samplerFilePath;
	}

	public static void setSamplerFilePath(String samplerFilePath) {
		CONSTANTS.samplerFilePath = samplerFilePath;
	}

	public static String getXsortedDataDir() {
		return XsortedDataDir;
	}

	public static void setXsortedDataDir(String xsortedDataDir) {
		XsortedDataDir = xsortedDataDir;
	}

	public static String getYsortedDataDir() {
		return YsortedDataDir;
	}

	public static void setYsortedDataDir(String ysortedDataDir) {
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
}