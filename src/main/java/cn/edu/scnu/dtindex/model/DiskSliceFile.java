package cn.edu.scnu.dtindex.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiskSliceFile implements WritableComparable<DiskSliceFile> {
    private Lob data;
    private IndexFile index;
    private boolean isIndexSliceFile;
    @Override
    public int compareTo(DiskSliceFile o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        data.write(out);
        index.write(out);
        out.writeBoolean(isIndexSliceFile);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Lob lobtmp = new Lob();
        lobtmp.readFields(in);
        this.data = lobtmp;
        IndexFile indexTmp = new IndexFile();
        indexTmp.readFields(in);
        this.index = indexTmp;
        this.isIndexSliceFile = in.readBoolean();
    }

    @Override
    public String toString() {
        return "DiskSliceFile{" +
                "data=" + data +
                ", index=" + index +
                ", isIndexSliceFile=" + isIndexSliceFile +
                '}';
    }
}
