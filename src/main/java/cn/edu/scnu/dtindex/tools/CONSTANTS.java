package cn.edu.scnu.dtindex.tools;


import java.io.*;

public class CONSTANTS implements Serializable {
    private static double HADOOP_BLOCK_SIZE = 128;//hadoop磁盘块大小
    private static double apha = 0.2;//索引所需空间的膨胀系数
    private static double numOfPartition;//分区数量
    private static int numOfEachdimention;//每一个维度的切分数=根号（分区数量）再取整
    private static final String constants_persistence_path = "/home/think/Desktop/data/contants.dat";//常量数据持久化路径
    private static long record_nums;//总记录数
    private static String dataFilePath = "/home/think/Desktop/data/data.txt";//数据文件
    private static String dataFileDir = "/home/think/Desktop/data";//数据路径
    private static String samplerFilePath = "/home/think/Desktop/data/sample/part-r-00000";//采样样文件路径
    private static String XsortedDataDir = CONSTANTS.getDataFileDir() + "/SampleSort/XSortTmp/";//x排序路径
    private static String YsortedDataDir = CONSTANTS.getDataFileDir() + "/SampleSort/YSortTmp/";//y排序路径
    private static long[] xPatitionsData = new long[CONSTANTS.getNumOfEachdimention() * 2];//保存x分界点
    private static long[][] yPatitionsData = new long[CONSTANTS.getNumOfEachdimention()][CONSTANTS.getNumOfEachdimention() * 2];//保存y分界点
    private static Double percentage = Double.parseDouble("10") / 100.00;//采样率

    public static void persistenceData() throws IOException {
        File file = new File(constants_persistence_path);
        if (!file.exists()) {
            file.createNewFile();
        } else {
            file.delete();
            file.createNewFile();
        }
        FileOutputStream fos = new FileOutputStream(file);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(new CONSTANTS());
        oos.flush();
        oos.close();
        fos.close();
    }

    public static void readPersistenceData() throws IOException, ClassNotFoundException {
        File file = new File(constants_persistence_path);
        if (!file.exists()) {
            file.createNewFile();
        }
        FileInputStream fis = new FileInputStream(file);
        ObjectInputStream ois = new ObjectInputStream(fis);
        CONSTANTS cos = (CONSTANTS) ois.readObject();
        ois.close();
        fis.close();

        showConstantsInfo();


    }

    public static void showConstantsInfo() {
        System.out.println("------------------------------------------【常量信息】------------------------------------------");
        System.out.println("|HADOOP_BLOCK_SIZE    		|hadoop磁盘块大小		|" + HADOOP_BLOCK_SIZE);
        System.out.println("|apha                 		|索引所需空间的膨胀系数	|" + apha);
        System.out.println("|numOfPartition       		|分区数量				|" + numOfPartition);
        System.out.println("|numOfEachdimention   		|每一个维度的切分数	|" + numOfEachdimention);
        System.out.println("|constants_persistence_path |常量数据持久化路径    |" + constants_persistence_path);
        System.out.println("|record_nums          		|总记录数				|" + record_nums);
        System.out.println("|dataFilePath       		|数据文件				|" + dataFilePath);
        System.out.println("|dataFileDir         		|数据路径				|" + dataFileDir);
        System.out.println("|samplerFilePath            |采样样文件路径		|" + samplerFilePath);
        System.out.println("|XsortedDataDir         	|x排序路径			|" + XsortedDataDir);
        System.out.println("|YsortedDataDir             |y排序路径		    |" + YsortedDataDir);
        System.out.println("|percentage                 |采样率		        |" + percentage);
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

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        CONSTANTS.setRecord_nums(100000);
        long[] x = {213421, 124321, 123412};
        long[][] y = {{4321, 2143, 6588}, {7688, 35653, 4532}, {87688, 8888, 456534}};
        CONSTANTS.setxPatitionsData(x);
        CONSTANTS.setyPatitionsData(y);
        persistenceData();
        readPersistenceData();
    }


    public static Double getPercentage() {
        return percentage;
    }

    public static void setPercentage(Double percentage) {
        CONSTANTS.percentage = percentage;
    }

    public static long[] getxPatitionsData() {
        return xPatitionsData;
    }

    public static void setxPatitionsData(long[] xPatitionsData) {
        CONSTANTS.xPatitionsData = xPatitionsData;
    }

    public static long[][] getyPatitionsData() {
        return yPatitionsData;
    }

    public static void setyPatitionsData(long[][] yPatitionsData) {
        CONSTANTS.yPatitionsData = yPatitionsData;
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

    public static String getDataFileDir() {
        return dataFileDir;
    }

    public static void setDataFileDir(String dataFileDir) {
        CONSTANTS.dataFileDir = dataFileDir;
    }

    public static String getDataFilePath() {
        return dataFilePath;
    }

    public static void setDataFilePath(String dataFilePath) {
        CONSTANTS.dataFilePath = dataFilePath;
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

    public static double getNumOfPartition() {
        return numOfPartition;
    }

    public static void setNumOfPartition(double numOfPartition) {
        CONSTANTS.numOfPartition = numOfPartition;
    }

    public static int getNumOfEachdimention() {
        return numOfEachdimention;
    }

    public static void setNumOfEachdimention(int numOfEachdimention) {
        CONSTANTS.numOfEachdimention = numOfEachdimention;
    }

    public static long getRecord_nums() {
        return record_nums;
    }

    public static void setRecord_nums(long record_nums) {
        CONSTANTS.record_nums = record_nums;
    }
}