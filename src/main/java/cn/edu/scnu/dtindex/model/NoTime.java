package cn.edu.scnu.dtindex.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author think 非时态信息元组
 */
public class NoTime implements WritableComparable<NoTime> {
	private String uuid;
	private String name;
	private String mail;
	private String cid;

	public NoTime() {
	}

	public NoTime(String uuid, String name, String mail, String cid) {
		super();
		this.uuid = uuid;
		this.name = name;
		this.mail = mail;
		this.cid = cid;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMail() {
		return mail;
	}

	public void setMail(String mail) {
		this.mail = mail;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	@Override
	public String toString() {
		return uuid + "," + name + "," + mail + "," + cid;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((cid == null) ? 0 : cid.hashCode());
		result = prime * result + ((mail == null) ? 0 : mail.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NoTime other = (NoTime) obj;
		if (cid == null) {
			if (other.cid != null)
				return false;
		} else if (!cid.equals(other.cid))
			return false;
		if (mail == null) {
			if (other.mail != null)
				return false;
		} else if (!mail.equals(other.mail))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (uuid == null) {
			if (other.uuid != null)
				return false;
		} else if (!uuid.equals(other.uuid))
			return false;
		return true;
	}

	/**
	 * 返回0，字符相同；
	 */
	@Override
	public int compareTo(NoTime noTime) {
		return this.uuid.compareTo(noTime.uuid);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.uuid = in.readUTF();
		this.name = in.readUTF();
		this.mail = in.readUTF();
		this.cid = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.uuid);
		out.writeUTF(this.name);
		out.writeUTF(this.mail);
		out.writeUTF(this.cid);

	}
}
