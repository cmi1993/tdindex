package cn.edu.scnu.dtindex.model;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class IndexFile implements WritableComparable<IndexFile> {
	private List<IndexRecord> records;
	private int recordSize;
    private Long indexOffset;//索引起始偏移量


	public IndexFile() {
	}

	public IndexFile(List<IndexRecord> records, Long indexOffset) {
		this.records = records;
		this.indexOffset = indexOffset;
		this.recordSize = records.size();
	}

	@Override
    public int compareTo(IndexFile o) {//indexFile不需要排序
        return 0;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {

		dataOutput.writeLong(indexOffset);
		dataOutput.writeInt(recordSize);
		for (IndexRecord record : records) {
		    record.write(dataOutput);
		}

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       this.indexOffset = dataInput.readLong();
       this.recordSize = dataInput.readInt();
       List<IndexRecord> list = new ArrayList<IndexRecord>(recordSize);
		for (int i = 0; i < this.recordSize; i++) {
			IndexRecord record = new IndexRecord();
			record.readFields(dataInput);
			list.add(record);
		}
		this.records = list;

    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append("--------------index info start-------------\n");
        sbuilder.append("indexOffset:"+indexOffset).append("\n");
		for (IndexRecord idxR :records ) {
		    sbuilder.append(idxR.toString()).append("\n");
		}
		sbuilder.append("--------------index info end  -------------\n");
        return sbuilder.toString();
    }
}
