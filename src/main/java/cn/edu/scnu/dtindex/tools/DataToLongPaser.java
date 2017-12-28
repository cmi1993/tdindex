package cn.edu.scnu.dtindex.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DataToLongPaser {
	public static long parse(String strTime) {
		Long longTime = null;
		try {
			longTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strTime).getTime();
		} catch (ParseException e1) {
			try {
				longTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(strTime).getTime();
			} catch (ParseException e2) {
				try {
					longTime = new SimpleDateFormat("yyyy-MM-dd").parse(strTime).getTime();
				} catch (ParseException e3) {
					e1.printStackTrace();
					e2.printStackTrace();
					e3.printStackTrace();
				}
			}
		}
		return longTime;
	}

	public static void main(String[] args) {

	}
}
