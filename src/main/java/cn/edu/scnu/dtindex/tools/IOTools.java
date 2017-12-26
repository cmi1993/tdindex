package cn.edu.scnu.dtindex.tools;

import java.io.*;

public class IOTools {
    /**
     * 写出数据
     *
     * @param str--要写出的数据(字符串)
     * @param path--写出的路径
     * @param flag--是否已追加的形式
     * @throws IOException
     */
    public static void toWrite(String str, String path, int flag) throws IOException {

        if (flag == 1) {
            File file = new File(path);// 指定要写入的文件
            if (!file.exists()) {// 如果文件不存在则创建
                file.createNewFile();
            }
            // 获取该文件的缓冲输出流
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file,true));
            bufferedWriter.write(str);
            bufferedWriter.flush();
            bufferedWriter.close();
        } else {
            File file = new File(path);// 指定要写入的文件
            if (!file.exists()) {// 如果文件不存在则创建
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            file.delete();
            // 获取该文件的缓冲输出流
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file,false));
            bufferedWriter.write(str);
            bufferedWriter.flush();
            bufferedWriter.close();
        }
    }

    /**
     * 按行读取数据
     *
     * @param path--要读取的数据的路径
     * @return 读出来的数据，字符串，以空格分隔
     * @throws IOException
     */
    public static String toRead(String path) throws IOException {
        String read = "";

        File filename = new File(path);

        StringBuffer address_array = new StringBuffer();

        FileReader f_in;
        f_in = new FileReader(filename);
        // 读取数据，装入address数组中
        BufferedReader bufread = new BufferedReader(f_in);
        while ((read = bufread.readLine()) != null) {
            address_array.append(read);
            address_array.append(" ");
        }
        read = address_array.toString();
        bufread.close();
        f_in.close();

        return read;

    }

    public static String toReadWithSpecialSplitSignal(String path) throws IOException {
        String read = "";

        File filename = new File(path);

        StringBuffer address_array = new StringBuffer();

        FileReader f_in;
        f_in = new FileReader(filename);
        // 读取数据，装入address数组中
        BufferedReader bufread = new BufferedReader(f_in);
        while ((read = bufread.readLine()) != null) {
            address_array.append(read);
            address_array.append("#");
        }
        read = address_array.toString();
        bufread.close();
        f_in.close();

        return read;

    }

}
