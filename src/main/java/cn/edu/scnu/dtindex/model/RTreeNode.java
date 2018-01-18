package cn.edu.scnu.dtindex.model;

import java.util.List;


/**
 * R树的内部节点
 */
public class RTreeNode{
	private boolean isLeaf=false;
	private int treeLevel;
	private List<RTreeNode> nodeList;//内部节点可以指向叶子节点或者内部节点
	private List<Tuple> leafData;
	private int nodeSize;
	private MBR mbr;

	public RTreeNode(int treeLevel, MBR mbr, List<Tuple> tmpList) {
		this.treeLevel = treeLevel;
		this.mbr = mbr;
		this.leafData = tmpList;
	}

	public RTreeNode(List<RTreeNode> nodeList, MBR mbr, int treeLevel) {
		this.treeLevel = treeLevel;
		this.nodeList = nodeList;
		this.mbr = mbr;
	}

	public RTreeNode(int rootLevel) {
		this.treeLevel = rootLevel;
	}


	public void clearTmpList(){
		leafData =null;
	}


	//-------------------GETTER_SETTER---------------------------------


	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isleaf) {
		this.isLeaf = isleaf;
	}

	public List<Tuple> getLeafData() {
		return leafData;
	}

	public void setLeafData(List<Tuple> leafData) {
		this.leafData = leafData;
	}

	public int getTreeLevel() {
		return treeLevel;
	}

	public void setTreeLevel(int treeLevel) {
		this.treeLevel = treeLevel;
	}

	public List<RTreeNode> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<RTreeNode> nodeList) {
		this.nodeList = nodeList;
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
