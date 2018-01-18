package cn.edu.scnu.dtindex.model;

import java.util.List;

public class RTreeSplitContainer {
	private List<Tuple> part1;
	private List<Tuple> part2;
	private MBR part1MBR;
	private MBR part2MBR;

	public RTreeSplitContainer(List<Tuple> part1, List<Tuple> part2, MBR part1MBR, MBR part2MBR) {
		this.part1 = part1;
		this.part2 = part2;
		this.part1MBR = part1MBR;
		this.part2MBR = part2MBR;
	}


	@Override
	public String toString() {
		return "RTreeSplitContainer{" +
				"part1=" + part1 +
				", part2=" + part2 +
				", part1MBR=" + part1MBR +
				", part2MBR=" + part2MBR +
				'}';
	}

	public List<Tuple> getPart1() {
		return part1;
	}

	public void setPart1(List<Tuple> part1) {
		this.part1 = part1;
	}

	public List<Tuple> getPart2() {
		return part2;
	}

	public void setPart2(List<Tuple> part2) {
		this.part2 = part2;
	}

	public MBR getPart1MBR() {
		return part1MBR;
	}

	public void setPart1MBR(MBR part1MBR) {
		this.part1MBR = part1MBR;
	}

	public MBR getPart2MBR() {
		return part2MBR;
	}

	public void setPart2MBR(MBR part2MBR) {
		this.part2MBR = part2MBR;
	}
}
