package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiskSliceFile implements WritableComparable<DiskSliceFile> {
	private Lob data;
	private IndexFile index;
	private boolean isIndexSliceFile;

	public DiskSliceFile() {
	}

	/**
	 * 磁盘切片的
	 *
	 * @param data
	 */
	public DiskSliceFile(Lob data) {
		this.data = data;
		this.isIndexSliceFile = false;
	}

	public DiskSliceFile(IndexFile index) {
		this.index = index;
		this.isIndexSliceFile = true;
	}

	@Override
	public int compareTo(DiskSliceFile o) {
		return 0;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isIndexSliceFile);
		if (isIndexSliceFile) {
			index.write(out);
		} else {
			data.write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isIndexSliceFile = in.readBoolean();
		if (isIndexSliceFile) {
			IndexFile indexTmp = new IndexFile();
			indexTmp.readFields(in);
			this.index = indexTmp;
		} else {
			Lob lobtmp = new Lob();
			lobtmp.readFields(in);
			this.data = lobtmp;
		}


	}

	@Override
	public String toString() {
		return "DiskSliceFile{" +
				"data=" + data +
				", index=" + index +
				", isIndexSliceFile=" + isIndexSliceFile +
				'}';
	}

	public Lob getData() {
		return data;
	}

	public void setData(Lob data) {
		this.data = data;
	}

	public IndexFile getIndex() {
		return index;
	}

	public void setIndex(IndexFile index) {
		this.index = index;
	}

	public boolean isIndexSliceFile() {
		return isIndexSliceFile;
	}

	public void setIndexSliceFile(boolean indexSliceFile) {
		isIndexSliceFile = indexSliceFile;
	}
}
