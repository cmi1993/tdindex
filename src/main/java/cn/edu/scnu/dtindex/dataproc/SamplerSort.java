package cn.edu.scnu.dtindex.dataproc;

import cn.edu.scnu.dtindex.model.Tuple;
import cn.edu.scnu.dtindex.tools.CONSTANTS;
import cn.edu.scnu.dtindex.tools.IOTools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SamplerSort {
	static List<String> xClassifiedPath;
	static List<String> yClassifiedPath;

	public static void sortX(String Rpath) throws IOException {
		String str = IOTools.toReadWithSpecialSplitSignal(Rpath);
		String[] records = str.split("#");
		List<Tuple> tuplesList = new ArrayList<Tuple>();
		long record_count = 0;
		for (String record : records) {
			String[] fields = record.split(",");
			Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
			tuplesList.add(t);
			record_count++;
		}
		Collections.sort(tuplesList);
		//3.计算分区数量
		CONSTANTS.setRecord_nums(record_count);
		File file = new File(CONSTANTS.getDataFilePath());
		long length = (file.length() / 1024 / 1024);
		double numOfPartition = length * (1 + CONSTANTS.getApha()) / CONSTANTS.getHadoopBlockSize();//分区数量
		long numOfEachdimention = Math.round(Math.sqrt(numOfPartition));//每个维度的分割数两
		CONSTANTS.setNumOfPartition(numOfPartition);
		CONSTANTS.setNumOfEachdimention((int) numOfEachdimention);
		long perXpartNum = CONSTANTS.getRecord_nums() / CONSTANTS.getNumOfEachdimention();//x维度每部分的切割数量

		long[] xparts = new long[CONSTANTS.getNumOfEachdimention() * 2];//保存x分界点
		//生成x排序临时文件分区路径
		xClassifiedPath = new ArrayList<String>();
		for (int i = 0; i < CONSTANTS.getNumOfEachdimention(); i++) {
			xClassifiedPath.add(CONSTANTS.getXsortedDataDir() + "/x_part_" + i);
		}
		//生成所有分区路径
		yClassifiedPath = new ArrayList<String>();
		for (int i = 0; i < CONSTANTS.getNumOfEachdimention(); i++) {
			for (int j = 0; j < CONSTANTS.getNumOfEachdimention(); j++) {
				yClassifiedPath.add(CONSTANTS.getYsortedDataDir() + "/Sort_part_" + i + "_" + j + "");
			}
		}
		StringBuilder stringBuilder = new StringBuilder();
		int j = 0;//用于获取分类路径的索引
		int k = 1;//用于记录写入x分区分界点数组的索引
		for (int i = 0; i < tuplesList.size(); i++) {
			if (i == 0) xparts[0] = tuplesList.get(0).getVt().getStart();
			if (i == tuplesList.size() - 1)
				xparts[xparts.length - 1] = tuplesList.get(tuplesList.size() - 1).getVt().getStart();

			stringBuilder.append(tuplesList.get(i).toString()).append("\n");
			if (i != 0 && i % (perXpartNum) == 0) {
				if (k<xparts.length-2) {
					xparts[k++] = tuplesList.get(i).getVt().getStart();//上一个分区的结束
					xparts[k++] = tuplesList.get(i + 1).getVt().getStart();//下一个分区的开始
				}
				IOTools.toWrite(stringBuilder.toString(), xClassifiedPath.get(j++), 0);
				stringBuilder = new StringBuilder();
			}
		}

		IOTools.toWrite(stringBuilder.toString(), xClassifiedPath.get(CONSTANTS.getNumOfEachdimention() - 1), 1);
		CONSTANTS.setxPatitionsData(xparts);
	}

	public static void sortY(String path) throws IOException {
		File filedir = new File(path);
		File[] files = filedir.listFiles();
		int x_part_fileCount = 0;
		long[][] yparts = new long[CONSTANTS.getNumOfEachdimention()][CONSTANTS.getNumOfEachdimention() * 2];//保存y分界点
		for (File file : files) {
			//System.out.println(file.getPath());
			String strs = IOTools.toReadWithSpecialSplitSignal(file.getPath());
			String[] records = strs.split("#");
			List<Tuple> tupleList = new ArrayList<Tuple>();
			long record_count = 0;
			for (String record : records) {
				String[] fields = record.split(",");
				Tuple t = new Tuple(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5]);
				tupleList.add(t);
				record_count++;
			}
			long perYpartCount = record_count / CONSTANTS.getNumOfEachdimention();//y排序每部分的数量
			Collections.sort(tupleList, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple o1, Tuple o2) {
					if (o1.getVt().getEnd() > o2.getVt().getEnd()) return 1;
					else if (o1.getVt().getEnd() > o2.getVt().getEnd()) return -1;
					else return 0;

				}
			});
			StringBuilder stringBuilder = new StringBuilder();
			int j = 0;//用于获取y分区写出路径的索引
			int k=1;//用于记录写入x分区分界点数组的索引

			for (int i = 0; i < tupleList.size(); i++) {
				if (i==0) yparts[x_part_fileCount][0]=tupleList.get(0).getVt().getEnd();
				if (i==tupleList.size()-1)yparts[x_part_fileCount][CONSTANTS.getNumOfEachdimention() * 2-1]=tupleList.get(tupleList.size()-1).getVt().getEnd();
				stringBuilder.append(tupleList.get(i).toString()).append("\n");
				if (i != 0 && i % (perYpartCount) == 0) {
					if (k<yparts[x_part_fileCount].length-2) {
						yparts[x_part_fileCount][k++] = tupleList.get(i).getVt().getEnd();
						yparts[x_part_fileCount][k++] = tupleList.get(i + 1).getVt().getEnd();
					}
					IOTools.toWrite(stringBuilder.toString(), yClassifiedPath.get(x_part_fileCount * CONSTANTS.getNumOfEachdimention() + j), 0);
					stringBuilder = new StringBuilder();
					j++;
				}
			}

			IOTools.toWrite(stringBuilder.toString(), yClassifiedPath.get(x_part_fileCount * CONSTANTS.getNumOfEachdimention() + CONSTANTS.getNumOfEachdimention() - 1), 1);
			x_part_fileCount++;
		}
		CONSTANTS.setyPatitionsData(yparts);
	}

	public static void main(String[] args) throws IOException {
		String pathToRead = CONSTANTS.getSamplerFilePath();
		System.out.println("x排序...");
		sortX(pathToRead);
		System.out.println("x排序成功...");

		pathToRead = CONSTANTS.getXsortedDataDir();
		System.out.println("y排序...");
		sortY(pathToRead);
		System.out.println("y排序成功...");
		CONSTANTS.persistenceData();
		CONSTANTS.showConstantsInfo();
	}
}
