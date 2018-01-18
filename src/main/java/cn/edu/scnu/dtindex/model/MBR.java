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
	 * @return
	 */
	public BigInteger mbrArea(){
		long x_span = topRight.getStart()-bottomLeft.getStart();
		long y_span = topRight.getEnd()-bottomLeft.getEnd();
		BigInteger x = new BigInteger(String.valueOf(x_span));
		BigInteger y = new BigInteger(String.valueOf(y_span));
		return x.multiply(y);
	}

	/**
	 * 判断和查询窗口是否相交
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
		ValidTime bL = interNodeList.get(0).getMbr().getBottomLeft();
		ValidTime tR = interNodeList.get(0).getMbr().getTopRight();
		for (RTreeNode node : interNodeList) {
			ValidTime vt = node.getMbr().getBottomLeft();
			if (vt.getStart() <= bL.getStart() && vt.getEnd() <= bL.getEnd()) {
				bL = vt;
			}
			if (vt.getStart() >= tR.getStart() && vt.getEnd() >= tR.getEnd()) {
				tR = vt;
			}
			vt = node.getMbr().getTopRight();
			if (vt.getStart() <= bL.getStart() && vt.getEnd() <= bL.getEnd()) {
				bL = vt;
			}
			if (vt.getStart() >= tR.getStart() && vt.getEnd() >= tR.getEnd()) {
				tR = vt;
			}
		}
		return new MBR(bL, tR);
	}

	public static MBR getLeafNodeMBR(List<RTreeNode> leafNodeList) {
		ValidTime bL = leafNodeList.get(0).getMbr().getBottomLeft();
		ValidTime tR = leafNodeList.get(0).getMbr().getTopRight();
		for (RTreeNode node : leafNodeList) {
			ValidTime vt = node.getMbr().getBottomLeft();
			if (vt.getStart() <= bL.getStart() && vt.getEnd() <= bL.getEnd()) {
				bL = vt;
			}
			if (vt.getStart() >= tR.getStart() && vt.getEnd() >= tR.getEnd()) {
				tR = vt;
			}
			vt = node.getMbr().getTopRight();
			if (vt.getStart() <= bL.getStart() && vt.getEnd() <= bL.getEnd()) {
				bL = vt;
			}
			if (vt.getStart() >= tR.getStart() && vt.getEnd() >= tR.getEnd()) {
				tR = vt;
			}
		}
		return new MBR(bL, tR);
	}

	public static MBR getTupleListMBR(List<Tuple> tupleList) {
		ValidTime bL = tupleList.get(0).getVt();
		ValidTime tR = tupleList.get(0).getVt();
		for (Tuple tuple : tupleList) {
			ValidTime vt = tuple.getVt();
			if (vt.getStart() <= bL.getStart() && vt.getEnd() <= bL.getEnd()) {
				bL = vt;
			}
			if (vt.getStart() >= tR.getStart() && vt.getEnd() >= tR.getEnd()) {
				tR = vt;
			}
		}
		return new MBR(bL, tR);
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
