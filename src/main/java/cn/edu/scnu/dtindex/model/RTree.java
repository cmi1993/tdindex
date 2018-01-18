package cn.edu.scnu.dtindex.model;

import java.util.List;

public class RTree {
	private RTreeNode root;
	private int maxSubtree;//该层每个子树的最大MBR个数
	private int maxNodeCapcity;//最大节点容量
	private int treeHight;//树的高度
	private long numOfNodes;

	public RTree(RTreeNode root, int maxSubtree, int maxNodeCapcity, long numOfNodes) {
		this.root = root;
		this.maxSubtree = maxSubtree;
		this.maxNodeCapcity = maxNodeCapcity;
		this.numOfNodes = numOfNodes;
		double hight = Math.ceil(Math.log(numOfNodes) / Math.log(maxNodeCapcity));
		this.treeHight = (int) Math.ceil(hight);
	}

	@Override
	public String toString() {
		return "RTree{" +
				"root=" + root +
				", maxSubtree=" + maxSubtree +
				", maxNodeCapcity=" + maxNodeCapcity +
				", treeHight=" + treeHight +
				", numOfNodes=" + numOfNodes +
				'}';
	}

	public void TraverseRTree(RTreeNode node){
		if (node.isLeaf()){
			List<Tuple> leafData = node.getLeafData();
			for (Tuple t : leafData) {
				System.out.println(t.toString());
			}
			return;
		}else {
			List<RTreeNode> nodeList = node.getNodeList();
			for ( RTreeNode n:nodeList ){
				TraverseRTree((RTreeNode) n);
			}
		}

	}


	public static void main(String[] args) {
		RTree rTree = new RTree(null,64/4,4,64);
		RTree rTree1= new RTree(null,64/8,8,64);
		RTree rTree2= new RTree(null,64/16,16,64);
		System.out.println(rTree.toString());
		System.out.println(rTree1.toString());
		System.out.println(rTree2.toString());
	}



	//-----------------------------GETTER SETTER-----------------------------

	public RTreeNode getRoot() {
		return root;
	}

	public void setRoot(RTreeNode root) {
		this.root = root;
	}

	public int getMaxSubtree() {
		return maxSubtree;
	}

	public void setMaxSubtree(int maxSubtree) {
		this.maxSubtree = maxSubtree;
	}

	public int getMaxNodeCapcity() {
		return maxNodeCapcity;
	}

	public void setMaxNodeCapcity(int maxNodeCapcity) {
		this.maxNodeCapcity = maxNodeCapcity;
	}

	public int getTreeHight() {
		return treeHight;
	}

	public void setTreeHight(int treeHight) {
		this.treeHight = treeHight;
	}

	public long getNumOfNodes() {
		return numOfNodes;
	}

	public void setNumOfNodes(long numOfNodes) {
		this.numOfNodes = numOfNodes;
	}
}
