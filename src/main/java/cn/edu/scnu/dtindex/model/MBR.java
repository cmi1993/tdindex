package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

/**
 * 最小限定矩形
 */
public class MBR implements WritableComparable<MBR> {
	private ValidTime bottomLeft;//左下
	private ValidTime topRight;//右上

	public MBR(ValidTime bottomLeft, ValidTime topRight) {
		this.bottomLeft = bottomLeft;
		this.topRight = topRight;
	}

	public MBR() {

	}


	/**
	 * 获取MBR面积
	 *
	 * @return
	 */
	public BigInteger mbrArea() {
		long x_span = topRight.getStart() - bottomLeft.getStart();
		long y_span = topRight.getEnd() - bottomLeft.getEnd();
		BigInteger x = new BigInteger(String.valueOf(x_span));
		BigInteger y = new BigInteger(String.valueOf(y_span));
		return x.multiply(y);
	}

	/**
	 * 判断和查询窗口是否相交
	 *
	 * @param query
	 * @return
	 */
	public boolean isIntersect(ValidTime query) {
		if ((topRight.getStart() >= query.getStart() && topRight.getEnd() <= query.getEnd())
				||
				(bottomLeft.getStart() >= query.getStart() && bottomLeft.getEnd() <= query.getEnd()))
			return true;
		else
			return false;
	}

	public static MBR getInterNodeMBR(List<RTreeNode> interNodeList) {

		long bottomLeft_start = Long.MAX_VALUE;
		long bottomLeft_end = Long.MAX_VALUE;
		long topRight_start = Long.MIN_VALUE;
		long topRight_end = Long.MIN_VALUE;

		for (RTreeNode node : interNodeList) {
			ValidTime vt = node.getMbr().getBottomLeft();
			if (bottomLeft_start >= vt.getStart()) bottomLeft_start = vt.getStart();
			if (bottomLeft_end >= vt.getEnd()) bottomLeft_end = vt.getEnd();
			if (topRight_start <= vt.getStart()) topRight_start = vt.getStart();
			if (topRight_end <= vt.getEnd()) topRight_end = vt.getEnd();
		}

		return new MBR(new ValidTime(bottomLeft_start, bottomLeft_end), new ValidTime(topRight_start, topRight_end));

	}

	public static MBR getLeafNodeMBR(List<RTreeNode> leafNodeList) {

		long bottomLeft_start = Long.MAX_VALUE;
		long bottomLeft_end = Long.MAX_VALUE;
		long topRight_start = Long.MIN_VALUE;
		long topRight_end = Long.MIN_VALUE;
		for (RTreeNode node : leafNodeList) {
			ValidTime vt = node.getMbr().getBottomLeft();
			if (bottomLeft_start >= vt.getStart()) bottomLeft_start = vt.getStart();
			if (bottomLeft_end >= vt.getEnd()) bottomLeft_end = vt.getEnd();
			if (topRight_start <= vt.getStart()) topRight_start = vt.getStart();
			if (topRight_end <= vt.getEnd()) topRight_end = vt.getEnd();

		}

		return new MBR(new ValidTime(bottomLeft_start, bottomLeft_end), new ValidTime(topRight_start, topRight_end));

	}

	/**
	 * 计算叶子节点的mbr，修正
	 *
	 * @param tupleList
	 * @return
	 */
	public static MBR getTupleListMBR(List<Tuple> tupleList) {
		long bottomLeft_start = Long.MAX_VALUE;
		long bottomLeft_end = Long.MAX_VALUE;
		long topRight_start = Long.MIN_VALUE;
		long topRight_end = Long.MIN_VALUE;
		for (Tuple tuple : tupleList) {
			ValidTime vt = tuple.getVt();
			if (bottomLeft_start >= vt.getStart()) bottomLeft_start = vt.getStart();
			if (bottomLeft_end >= vt.getEnd()) bottomLeft_end = vt.getEnd();
			if (topRight_start <= vt.getStart()) topRight_start = vt.getStart();
			if (topRight_end <= vt.getEnd()) topRight_end = vt.getEnd();
		}
		return new MBR(new ValidTime(bottomLeft_start, bottomLeft_end), new ValidTime(topRight_start, topRight_end));
	}

	@Override
	public int compareTo(MBR o) {//MBR暂时不需要进行比较
		return 0;
	}

	@Override
	public String toString() {
		return "MBR{" +
				"bottomLeft=" + bottomLeft +
				", topRight=" + topRight +
				'}';
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.bottomLeft.write(out);
		this.topRight.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ValidTime bL = new ValidTime();
		bL.readFields(in);
		this.bottomLeft = bL;
		ValidTime tR = new ValidTime();
		tR.readFields(in);
		this.topRight = tR;
	}

	//------------------------------------GETTER SETTER------------------------------------------


	public ValidTime getBottomLeft() {
		return bottomLeft;
	}

	public void setBottomLeft(ValidTime bottomLeft) {
		this.bottomLeft = bottomLeft;
	}

	public ValidTime getTopRight() {
		return topRight;
	}

	public void setTopRight(ValidTime topRight) {
		this.topRight = topRight;
	}
}
