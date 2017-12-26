package cn.edu.scnu.dtindex.bigdata.seqfile;

import cn.edu.scnu.dtindex.model.Lob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class MapSerialization implements WritableComparable<MapSerialization>{
    private MapWritable map;

    public MapSerialization() {
    }

    public MapSerialization(MapWritable map) {
        this.map = map;
    }

    @Override
    public int compareTo(MapSerialization o) {
            return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        map.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        MapWritable mm = new MapWritable();
        mm.readFields(dataInput);
        this.map=mm;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        str.append("------------------Map start-------------------\n");
        for (Map.Entry<Writable, Writable> entrySet:map.entrySet()) {
            str.append("{key:").append(entrySet.getKey()).append(",value:").append(entrySet.getValue()).append("}\n");
        }
        str.append("------------------Map end-------------------");
        return str.toString();
    }

    public static void main(String[] args) throws IOException {
        String spath = "/Users/think/Desktop/seqMap.seq";
        ToSerialize(spath);
        DeSerialize(spath);

    }

    private static void DeSerialize(String spath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(spath);
        Text key = new Text();
        MapSerialization value = new MapSerialization();
        SequenceFile.Reader reader = null;

        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

        while (reader.next(key, value)) {
            System.out.println("KEY-"+key);
            System.out.println(value.toString());

        }
        IOUtils.closeStream(reader);
    }

    private static void ToSerialize(String spath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(spath);
        MapWritable map = new MapWritable();
        map.put(new IntWritable(123),new Text("text value"));
        map.put(new IntWritable(1000),new Text("txt value of int key"));
        map.put(new Text("TExt key"),new IntWritable(23));


        Text key  =new Text();
        MapSerialization value = new MapSerialization(map);
        SequenceFile.Writer writer = null;

        writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()), SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

        key.set("key------");
        writer.append(key,value);
        IOUtils.closeStream(writer);
        System.out.println("successfully");
    }
}
