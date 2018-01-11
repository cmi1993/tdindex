package cn.edu.scnu.dtindex.entrance;

import cn.edu.scnu.dtindex.dataproc.ClassifiedDataIntoSlice;
import cn.edu.scnu.dtindex.dataproc.DataGenerate;
import cn.edu.scnu.dtindex.dataproc.Sampler;

import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Scanner sin = new Scanner(System.in);
		while(sin.nextInt()!=0){
			ShowMenu();
		}

        





    }

	private static void ShowMenu() {
		System.out.println("------------------");
		System.out.println("|1|生成数据");
		System.out.println("|2|采样数据");
		System.out.println("|3|排序样本");
		System.out.println("|4|建立索引");
		System.out.println("|5|窗口查询");
		System.out.println("|6|时态投影");
		System.out.println("|7|时态连接");
		System.out.println("------------------");
	}
}
