package cn.edu.scnu.dtindex.model;


import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;
import java.util.Map;
import java.util.Set;

public class IndexFile implements WritableComparable<IndexFile> {

    private MapWritable indexMap ;//<Long,IndexRecord> lobid,IndexRecord
    private Long indexOffset;//索引起始偏移量

    @Override
    public int compareTo(IndexFile o) {//indexFile不需要排序
        return 0;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        indexMap.write(dataOutput);
        dataOutput.writeLong(indexOffset);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       MapWritable map = new MapWritable();
       map.readFields(dataInput);
       indexMap=map;
       this.indexOffset = dataInput.readLong();
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        Set<Map.Entry<Writable, Writable>> entrySet = indexMap.entrySet();
        sbuilder.append("--------------index info-------------\n");
        sbuilder.append("indexOffset:"+indexOffset).append("\n");
        for (Map.Entry<Writable, Writable> entry :
                entrySet) {
            sbuilder.append("{lobid:"+entry.getKey()+":"+entry.getValue()+"}\n");

        }
        return sbuilder.toString();
    }
}
