package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RTreeDiskSliceFile implements WritableComparable<RTreeDiskSliceFile> {
	private RTree index;
	private RTreeNode leafData;
	private boolean isIndexFile;

	public RTreeDiskSliceFile(RTree index) {
		this.index = index;
		this.isIndexFile = true;
	}

	public RTreeDiskSliceFile(RTreeNode leafData) {
		this.leafData = leafData;
		this.isIndexFile = false;
	}

	public RTreeDiskSliceFile() {

	}

	@Override
	public int compareTo(RTreeDiskSliceFile o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isIndexFile);
		if (isIndexFile) {
			index.write(out);
		}else {
			leafData.write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isIndexFile = in.readBoolean();
		if (isIndexFile){
			RTree index = new RTree();
			index.readFields(in);
			this.index = index;
		}else {
			RTreeNode node = new RTreeNode();
			node.readFields(in);
			this.leafData = node;
		}
	}
}
