package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * R树的内部节点
 */
public class RTreeNode implements WritableComparable<RTreeNode> {
	private boolean isroot = false;
	private boolean isLeaf = false;
	private boolean isIndex = false;
	private int treeLevel;
	private int nodeSize;
	private List<RTreeNode> nodeList;//内部节点可以指向叶子节点或者内部节点
	private int leafDataSize;
	private List<Tuple> leafData;
	private MBR mbr;
	private long offset;

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

	public RTreeNode() {

	}

	@Override
	public int compareTo(RTreeNode o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (isIndex) {//如果是索引节点
			out.writeBoolean(isLeaf);
			out.writeBoolean(isIndex);
			out.writeInt(treeLevel);
			out.writeLong(offset);
			mbr.write(out);
		} else if (isLeaf) {//如果是叶子节点
			out.writeBoolean(isLeaf);
			out.writeBoolean(isLeaf);
			out.writeInt(treeLevel);
			out.writeInt(leafData.size());
			out.writeLong(offset);
			for (Tuple t : leafData) {
				t.write(out);
			}
			mbr.write(out);
		} else if (isroot) {//如果是根节点

			out.writeBoolean(isLeaf);
			out.writeBoolean(isLeaf);
			out.writeInt(treeLevel);
			out.writeInt(nodeSize);
			for (RTreeNode r : nodeList) {
				r.write(out);
			}
		} else {//如果是内部节点
			out.writeBoolean(isLeaf);
			out.writeBoolean(isLeaf);
			out.writeInt(treeLevel);
			out.writeInt(nodeSize);
			for (RTreeNode r : nodeList) {
				r.write(out);
			}
			mbr.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isLeaf = in.readBoolean();
		this.isIndex = in.readBoolean();
		if (isIndex) {
			this.treeLevel = in.readInt();
			this.offset = in.readLong();
			MBR mbr = new MBR();
			mbr.readFields(in);
			this.mbr = mbr;
		} else if (isLeaf) {
			this.treeLevel = in.readInt();
			this.leafDataSize = in.readInt();
			this.offset = in.readLong();
			List<Tuple> leafDatalist = new ArrayList<Tuple>(leafDataSize);
			for (int i = 0; i < leafDataSize; i++) {
				Tuple t = new Tuple();
				t.readFields(in);
				leafDatalist.add(t);
			}
			this.leafData = leafDatalist;
			MBR mbr = new MBR();
			mbr.readFields(in);
			this.mbr = mbr;
		} else if (isroot) {
			this.treeLevel = in.readInt();
			this.nodeSize = in.readInt();
			List<RTreeNode> nodelist = new ArrayList<RTreeNode>(nodeSize);
			for (int i = 0; i < nodeSize; i++) {
				RTreeNode node = new RTreeNode();
				node.readFields(in);
				nodelist.add(node);
			}
			this.nodeList = nodelist;
		} else {
			this.treeLevel = in.readInt();
			this.nodeSize = in.readInt();
			List<RTreeNode> nodelist = new ArrayList<RTreeNode>(nodeSize);
			for (int i = 0; i < nodeSize; i++) {
				RTreeNode node = new RTreeNode();
				node.readFields(in);
				nodelist.add(node);
			}
			this.nodeList = nodelist;
			MBR mbr = new MBR();
			mbr.readFields(in);
			this.mbr = mbr;
		}
	}

	public void clearLeafDataList() {
		leafData = null;
	}


	//-------------------GETTER_SETTER---------------------------------


	public boolean isIsroot() {
		return isroot;
	}

	public void setIsroot(boolean isroot) {
		this.isroot = isroot;
	}

	public boolean isIndex() {
		return isIndex;
	}

	public void setIndex(boolean index) {
		isIndex = index;
	}

	public int getLeafDataSize() {
		return leafDataSize;
	}

	public void setLeafDataSize(int leafDataSize) {
		this.leafDataSize = leafDataSize;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

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
