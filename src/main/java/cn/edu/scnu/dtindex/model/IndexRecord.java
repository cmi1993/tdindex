package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexRecord implements WritableComparable<IndexRecord> {
    private String lobid;
    private ValidTime maxNode;
    private ValidTime minNode;
    private Long lob_offset;

    @Override
    public int compareTo(IndexRecord o) {//索引记录的排序
        return this.maxNode.compareTo(o.maxNode);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(lobid);
        maxNode.write(out);
        minNode.write(out);
        out.writeLong(lob_offset);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.lobid = in.readUTF();
        ValidTime max = new ValidTime();
        max.readFields(in);
        this.maxNode =max;
        ValidTime min = new ValidTime();
        min.readFields(in);
        this.minNode = min;
        this.lob_offset = in.readLong();
    }

    @Override
    public String toString() {
        return "IndexRecord{" +
                "lobid='" + lobid + '\'' +
                ", maxNode=" + maxNode +
                ", minNode=" + minNode +
                ", lob_offset=" + lob_offset +
                '}';
    }
}
