package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class RTree implements WritableComparable<RTree> {
	private RTreeNode root;
	private int maxSubtree;//该层每个子树的最大MBR个数
	private int maxNodeCapcity;//最大节点容量
	private int treeHight;//树的高度
	private long numOfNodes;
	private long indexBeginOffset;
	private String diskFileName="";

	public RTree(RTreeNode root, int maxSubtree, int maxNodeCapcity, long numOfNodes) {
		this.root = root;
		this.maxSubtree = maxSubtree;
		this.maxNodeCapcity = maxNodeCapcity;
		this.numOfNodes = numOfNodes;
		double hight = Math.ceil(Math.log(numOfNodes) / Math.log(maxNodeCapcity));
		this.treeHight = (int) Math.ceil(hight);
	}
	public RTree() {

	}

	@Override
	public String toString() {
		return "RTree{" +
				"root=" + root +
				", maxSubtree=" + maxSubtree +
				", maxNodeCapcity=" + maxNodeCapcity +
				", treeHight=" + treeHight +
				", numOfNodes=" + numOfNodes +
				", indexBeginOffset=" + indexBeginOffset +
				", diskFileName='" + diskFileName + '\'' +
				'}';
	}

	@Override
	public int compareTo(RTree o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		root.write(out);
		out.writeInt(maxSubtree);
		out.writeInt(maxNodeCapcity);
		out.writeInt(treeHight);
		out.writeLong(numOfNodes);
		out.writeLong(indexBeginOffset);
		out.writeUTF(diskFileName);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		RTreeNode root = new RTreeNode();
		root.readFields(in);
		this.root =root;
		this.maxSubtree = in.readInt();
		this.maxNodeCapcity = in.readInt();
		this.treeHight = in.readInt();
		this.numOfNodes = in.readLong();
		this.indexBeginOffset = in.readLong();
		this.diskFileName = in.readUTF();
	}

	public void TraverseRTree(RTreeNode node) {
		if (node.isLeaf()) {
			List<Tuple> leafData = node.getLeafData();
			for (Tuple t : leafData) {
				System.out.println(t.toString());
			}
			return;
		} else {
			List<RTreeNode> nodeList = node.getNodeList();
			for (RTreeNode n : nodeList) {
				TraverseRTree((RTreeNode) n);
			}
		}

	}


	public static void main(String[] args) {
		RTree rTree = new RTree(null, 64 / 4, 4, 64);
		RTree rTree1 = new RTree(null, 64 / 8, 8, 64);
		RTree rTree2 = new RTree(null, 64 / 16, 16, 64);
		System.out.println(rTree.toString());
		System.out.println(rTree1.toString());
		System.out.println(rTree2.toString());
	}


	//-----------------------------GETTER SETTER-----------------------------

	public long getIndexBeginOffset() {
		return indexBeginOffset;
	}

	public void setIndexBeginOffset(long indexBeginOffset) {
		this.indexBeginOffset = indexBeginOffset;
	}

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

	public String getDiskFileName() {
		return diskFileName;
	}

	public void setDiskFileName(String diskFileName) {
		this.diskFileName = diskFileName;
	}
}
