package cn.edu.scnu.dtindex.model;

import java.util.List;

public class RTreeSplitSlice {
	private List<Tuple> tuples;
	private int splitCount;
	private MBR sliceMBR;

	public RTreeSplitSlice() {
	}

	public RTreeSplitSlice(List<Tuple> tuples, int splitCount, MBR sliceMBR) {
		this.tuples = tuples;
		this.splitCount = splitCount;
		this.sliceMBR = sliceMBR;
	}

	public List<Tuple> getTuples() {
		return tuples;
	}

	public void setTuples(List<Tuple> tuples) {
		this.tuples = tuples;
	}

	public int getSplitCount() {
		return splitCount;
	}

	public void setSplitCount(int splitCount) {
		this.splitCount = splitCount;
	}

	public MBR getSliceMBR() {
		return sliceMBR;
	}

	public void setSliceMBR(MBR sliceMBR) {
		this.sliceMBR = sliceMBR;
	}
}
