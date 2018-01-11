package cn.edu.scnu.dtindex.model;

public class Course {
	private int cid;
	private String teacherName;
	private long start_time;
	private long end_time;

	public Course(int cid, String teacherName, long start_time, long end_time) {
		this.cid = cid;
		this.teacherName = teacherName;
		this.start_time = start_time;
		this.end_time = end_time;
	}

	public Course() {
	}

	public Course(String cid, String teacherName, String start_time, String end_time) {
		this.cid = Integer.parseInt(cid);
		this.teacherName = teacherName;
		this.start_time = Long.parseLong(start_time);
		this.end_time = Long.parseLong(end_time);
	}


	public int getCid() {
		return cid;
	}

	public void setCid(int cid) {
		this.cid = cid;
	}

	public String getTeacherName() {
		return teacherName;
	}

	public void setTeacherName(String teacherName) {
		this.teacherName = teacherName;
	}

	public long getStart_time() {
		return start_time;
	}

	public void setStart_time(long start_time) {
		this.start_time = start_time;
	}

	public long getEnd_time() {
		return end_time;
	}

	public void setEnd_time(long end_time) {
		this.end_time = end_time;
	}

	@Override
	public String toString() {
		return cid +
				"," + teacherName +
				"," + start_time +
				"," + end_time;
	}
}
