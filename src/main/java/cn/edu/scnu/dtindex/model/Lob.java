package cn.edu.scnu.dtindex.model;

import cn.edu.scnu.dtindex.tools.UUIDGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Lob implements WritableComparable<Lob> {
	private String lobid;
	private List<Tuple> loblist;
	private ValidTime maxNode;
	private ValidTime minNode;
	private int numOfTuple;


	public Lob() {
	}

	public Lob(List<Tuple> loblist, ValidTime maxNode, ValidTime minNode) {
		this.lobid = UUIDGenerator.getUUID();
		this.loblist = loblist;
		this.maxNode = maxNode;
		this.minNode = minNode;
		this.numOfTuple = loblist.size();
	}

	public Lob(List<Tuple> loblist) {
		this.lobid = UUIDGenerator.getUUID();
		this.loblist = loblist;
		this.maxNode = loblist.get(0).getVt();
		this.minNode = loblist.get(loblist.size() - 1).getVt();
		this.numOfTuple = loblist.size();
	}

	@Override
	public int compareTo(Lob o) {//lob的排序方式
		return maxNode.compareTo(o.maxNode);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(lobid);
		maxNode.write(out);
		minNode.write(out);
		out.writeInt(numOfTuple);
		for (Tuple t : loblist) {
			t.write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.lobid = in.readUTF();
		ValidTime max = new ValidTime();
		max.readFields(in);
		this.maxNode = max;
		ValidTime min = new ValidTime();
		min.readFields(in);
		this.minNode = min;
		this.numOfTuple = in.readInt();
		List<Tuple> list = new ArrayList<Tuple>();
		Tuple t;
		for (int i = 0; i < numOfTuple; i++) {
			t = new Tuple();
			t.readFields(in);
			list.add(t);
		}
		this.loblist = list;

	}

	@Override
	public String toString() {

		StringBuilder str = new StringBuilder();
		str.append("-----------------lob_list----------------\n")
				.append("lobid:").append(lobid).append("\n")
				.append("maxNode:").append(maxNode).append("\n")
				.append("minNode:").append(minNode).append("\n")
				.append("numOfTuple:").append(numOfTuple).append("\n")
				.append("----------------\n");
		for (Tuple t :
				this.loblist) {
			str.append(t.toString()).append("\n");
		}
		str.append("-----------------lob_list----------------\n");

		return str.toString();
	}

	/**
	 * 二分查找该线序分支
	 *
	 * @param query
	 * @return
	 */
	public List<Tuple> BinarySearchInLob(ValidTime query) {
		if (minNode.isMisPlace(query) || minNode.isContainPeriod(query))//如果最小的节点都和查询窗口错位,或者最小的都包含查询窗口
			return null;
		else if (query.isContainPeriod(maxNode))//如果最大的都被包含，整个LOB都被包含
			return loblist;
		else {//二分查找第一个被包含的线序
			int start = 0;
			int end = loblist.size() - 1;
			List<Tuple> result = new ArrayList<Tuple>(end / 2);
			while (start <= end) {
				int mid = (start + end) / 2;
				if (query.isContainPeriod(loblist.get(mid).getVt())) {
					result.addAll(0,loblist.subList(mid, end+1));
					end = mid - 1;
				} else {
					start = mid + 1;
				}
			}
			return result;

		}

	}


	public static void main(String[] args) throws IOException {
		//ToSerialization("/Users/think/Desktop/lob.seq");
		//DeSerialization("/Users/think/Desktop/lob.seq");
		Tuple t1 = new Tuple(new Object[]{"", "", "", ""}, new long[]{1, 10});
		Tuple t2 = new Tuple(new Object[]{"", "", "", ""}, new long[]{1, 9});
		Tuple t3 = new Tuple(new Object[]{"", "", "", ""}, new long[]{1, 8});
		Tuple t4 = new Tuple(new Object[]{"", "", "", ""}, new long[]{1, 7});
		Tuple t5 = new Tuple(new Object[]{"", "", "", ""}, new long[]{2, 7});
		Tuple t6 = new Tuple(new Object[]{"", "", "", ""}, new long[]{2, 6});
		Tuple t7 = new Tuple(new Object[]{"", "", "", ""}, new long[]{2, 5});
		Tuple t8 = new Tuple(new Object[]{"", "", "", ""}, new long[]{2, 4});
		Tuple t9 = new Tuple(new Object[]{"", "", "", ""}, new long[]{3, 4});
		Tuple t10 = new Tuple(new Object[]{"", "", "", ""}, new long[]{4, 4});

		List<Tuple> list = new ArrayList<Tuple>();
		list.add(t1);
		list.add(t2);
		list.add(t3);
		list.add(t4);
		list.add(t5);
		list.add(t6);
		list.add(t7);
		list.add(t8);
		list.add(t9);
		list.add(t10);
		Lob lob = new Lob(list);
		List<Tuple> tuples = lob.BinarySearchInLob(new ValidTime(1, 7));
		for (Tuple t : tuples) {
			System.out.print("<" + t.getVt().getStart() + "," + t.getVt().getEnd() + ">");
		}
	}

	private static void DeSerialization(String spath) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(spath);
		Text key = new Text();
		Lob value = new Lob();
		SequenceFile.Reader reader = null;

		reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

		while (reader.next(key, value)) {
			System.out.println("KEY-" + key);
			System.out.println(value.toString());

		}
		IOUtils.closeStream(reader);
	}


	private static void ToSerialization(String spath) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(spath);

		Text key = new Text();
		Lob value = new Lob();
		SequenceFile.Writer writer = null;

		writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()),
				SequenceFile.Writer.valueClass(value.getClass()), SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

		//---------------lob1--------
		Tuple lob1_t1 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "lob1_maxnode_t1", "HarryTorres@yahoo.com.cn", "100094", "2026-10-01 15:34:20", "2029-02-04 03:23:33");
		Tuple lob1_t2 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob1_t2", "AnthonyColeman@@mail.com", "100070", "1980-05-31 18:48:40", "2005-11-11 22:59:44");
		Tuple lob1_t3 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob1_t3", "AnthonyColeman@@mail.com", "100070", "1984-08-11 15:14:55", "2005-05-31 09:28:23");
		Tuple lob1_t4 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob1_t4", "AnthonyColeman@@mail.com", "100070", "2016-02-20 01:59:25", "2028-05-15 02:36:15");
		Tuple lob1_t5 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob1_min_t5", "AnthonyColeman@@mail.com", "100070", "2028-02-29 07:59:44", "2028-03-25 00:55:03");
		List<Tuple> lob1list = new ArrayList<Tuple>();
		lob1list.add(lob1_t1);
		lob1list.add(lob1_t2);
		lob1list.add(lob1_t3);
		lob1list.add(lob1_t4);
		lob1list.add(lob1_t5);
		Lob lob1 = new Lob(lob1list);

		//---------------lob2--------
		Tuple lob2_t1 = new Tuple("0b8ce36f349741eab4de6a1dfc1f1b7d", "lob2_maxnode_t1", "HarryTorres@yahoo.com.cn", "100094", "2026-10-01 15:34:20", "2029-02-04 03:23:33");
		Tuple lob2_t2 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob2_t2", "AnthonyColeman@@mail.com", "100070", "1980-05-31 18:48:40", "2005-11-11 22:59:44");
		Tuple lob2_t3 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob2_t3", "AnthonyColeman@@mail.com", "100070", "1984-08-11 15:14:55", "2005-05-31 09:28:23");
		Tuple lob2_t4 = new Tuple("fa7c65393a5d4a06a4e39dc50d6f0a54", "lob2_min_t4", "AnthonyColeman@@mail.com", "100070", "2016-02-20 01:59:25", "2028-05-15 02:36:15");

		List<Tuple> lob2list = new ArrayList<Tuple>();
		lob2list.add(lob2_t1);
		lob2list.add(lob2_t2);
		lob2list.add(lob2_t3);
		lob2list.add(lob2_t4);

		Lob lob2 = new Lob(lob2list);

		key.set("Lob1");
		writer.append(key, lob1);
		key.set("lob2");
		writer.append(key, lob2);

		IOUtils.closeStream(writer);
		System.out.println("successfully");
	}

	public String getLobid() {
		return lobid;
	}

	public void setLobid(String lobid) {
		this.lobid = lobid;
	}

	public List<Tuple> getLoblist() {
		return loblist;
	}

	public void setLoblist(List<Tuple> loblist) {
		this.loblist = loblist;
	}

	public ValidTime getMaxNode() {
		return maxNode;
	}

	public void setMaxNode(ValidTime maxNode) {
		this.maxNode = maxNode;
	}

	public ValidTime getMinNode() {
		return minNode;
	}

	public void setMinNode(ValidTime minNode) {
		this.minNode = minNode;
	}

	public int getNumOfTuple() {
		return numOfTuple;
	}

	public void setNumOfTuple(int numOfTuple) {
		this.numOfTuple = numOfTuple;
	}
}
