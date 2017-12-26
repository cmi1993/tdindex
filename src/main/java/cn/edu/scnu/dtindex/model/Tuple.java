package cn.edu.scnu.dtindex.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;



/**
 * @author think 基本数据条目类
 */
public class Tuple implements WritableComparable<Tuple> {
	private NoTime nt;// 非时态元组
	private ValidTime vt;// 有效时间

	public Tuple() {
	}

	// obj包含时态和非时态
	public Tuple(Object[] obj) {
		if (obj.length != 6) {
			return;
		}
		Object[] ntObj = new Object[obj.length - 2];
		Object[] vtObj = new Object[2];
		System.arraycopy(obj, 0, ntObj, 0, ntObj.length);
		System.arraycopy(obj, obj.length - 2, vtObj, 0, vtObj.length);
		nt = new NoTime(ntObj[0] + "", ntObj[1] + "", ntObj[2] + "", ntObj[3] + "");
		vt = new ValidTime(vtObj[0] + "", vtObj[1] + "");
	}

	public Tuple(Object uuid, Object name, Object cid, Object mail, Object start_time, Object end_time) {
		this.nt = new NoTime(uuid + "", name + "", cid + "", mail + "");
		this.vt = new ValidTime(start_time + "", end_time + "");
	}

	public Tuple(Object[] nt, long[] vt) {
		this.nt = new NoTime(nt[0] + "", nt[1] + "", nt[2] + "", nt[3] + "");
		this.vt = new ValidTime(vt[0], vt[1]);
	}

	public long getStart() {
		return vt.getStart();
	}

	public long getEnd() {
		return vt.getEnd();
	}

	public NoTime getNt() {
		return nt;
	}

	public void setNt(NoTime nt) {
		this.nt = nt;
	}

	public ValidTime getVt() {
		return vt;
	}

	public void setVt(ValidTime vt) {
		this.vt = vt;
	}

	@Override
	public String toString() {
		return nt.toString() + "," + vt.toString();
	}

	@Override
	public int compareTo(Tuple o) {
		return this.vt.compareTo(o.vt);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		NoTime notime = new NoTime();
		notime.readFields(in);
		this.nt = notime;
		ValidTime vTime = new ValidTime();
		vTime.readFields(in);
		this.vt = vTime;

	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.nt.write(out);
		this.vt.write(out);
	}

	public static void main(String[] args) throws IOException {
		ToSerialization("/Users/think/Desktop/seqTuple.seq");
		DeSerialization("/Users/think/Desktop/seqTuple.seq");
	}
	
	private static void DeSerialization(String spath) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(spath);
		Text key = new Text();
		Tuple value = new Tuple();
		Reader reader = null;
		
		reader = new Reader(conf, Reader.file(path));
		
		while (reader.next(key, value)) {
			
			System.out.println("KEY-" + key + ",VALUE--" + value.toString());
			
		}
		IOUtils.closeStream(reader);
	}

	public static void ToSerialization(String spath) throws IOException{
		Configuration conf = new Configuration();
		Path path = new Path(spath);

		Text key = new Text();
		Tuple value = new Tuple();
		Writer writer = null;

		writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(key.getClass()),
				Writer.valueClass(value.getClass()), Writer.compression(CompressionType.NONE));

		Tuple t1 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "Zachary Rice", "HarryTorres@yahoo.com.cn", "100094",
				"2026-10-01 15:34:20", "2029-02-04 03:23:33");
		key.set("t1");
		writer.append(key, t1);
		System.out.print("["+writer.getLength()+"]");
		System.out.println(t1.toString());
		Tuple t2 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "Zachary Rice", "HarryTorres@yahoo.com.cn", "100094",
				"2024-03-07 02:35:41", "2029-07-10 18:34:31");
		key.set("t2");
		writer.append(key, t2);
		System.out.print("["+writer.getLength()+"]");
		System.out.println(t2.toString());
		Tuple t3 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "Zachary Rice", "HarryTorres@yahoo.com.cn", "100094",
				"2016-10-10 14:29:31", "2018-08-09 13:55:53");
		key.set("t3");
		writer.append(key, t3);
		System.out.print("["+writer.getLength()+"]");
		System.out.println(t3.toString());
		Tuple t4 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "Zachary Rice", "HarryTorres@yahoo.com.cn", "100094",
				"1991-08-10 13:09:11", "2016-07-10 11:25:23");
		key.set("t4");
		writer.append(key, t4);
		System.out.print("["+writer.getLength()+"]");
		System.out.println(t4.toString());
		Tuple t5 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "Zachary Rice", "HarryTorres@yahoo.com.cn", "100094",
				"2000-10-15 09:14:42", "2004-02-21 02:12:18");
		key.set("t5");
		writer.append(key, t5);
		System.out.print("["+writer.getLength()+"]");
		System.out.println(t5.toString());
		IOUtils.closeStream(writer);
		System.out.println("successfully");
	}

}