package cn.edu.scnu.dtindex.model;

public class TreeNode {
	private MBR mbr;
	private boolean isLeafNode;

	public MBR getMbr() {
		return mbr;
	}

	public void setMbr(MBR mbr) {
		this.mbr = mbr;
	}

	public boolean isLeafNode() {
		return isLeafNode;
	}

	public void setLeafNode(boolean leafNode) {
		isLeafNode = leafNode;
	}
}
