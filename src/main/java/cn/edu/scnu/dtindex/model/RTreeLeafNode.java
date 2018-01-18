package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * R树的叶子节点
 */
public class RTreeLeafNode extends TreeNode implements WritableComparable<RTreeLeafNode> {
	private final boolean isLeafNode=true;
	private List<Tuple> tuples;
	private int nodeSize;
	private MBR mbr;

	public RTreeLeafNode(List<Tuple> tuples) {
		this.tuples = tuples;
		this.nodeSize = tuples.size();
		this.mbr = MBR.getTupleListMBR(tuples);
	}

	@Override
	public int compareTo(RTreeLeafNode o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {

	}

	@Override
	public void readFields(DataInput in) throws IOException {

	}


	public boolean isLeafNode() {
		return isLeafNode;
	}

	public List<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(List<Tuple> tuples) {
		this.tuples = tuples;
	}

	public int getNodeSize() {
		return nodeSize;
	}

	public void setNodeSize(int nodeSize) {
		this.nodeSize = nodeSize;
	}

	public MBR getMbr() {
		return mbr;
	}

	public void setMbr(MBR mbr) {
		this.mbr = mbr;
	}
}
