package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RTreeIndexFile implements WritableComparable<RTreeIndexFile> {

	private RTree index;//without tuple

	public RTreeIndexFile(RTree index) {
		this.index = index;
	}

	@Override
	public int compareTo(RTreeIndexFile o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		index.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		RTree rtee = new RTree();
		rtee.readFields(in);
	}
}
