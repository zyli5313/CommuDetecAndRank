package kmeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
//import static org.junit.Assert.*;

public class InstanceReduce2 {
  public static class InstanceReducer2 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable id, Iterator<Text> instances,
            OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      int num = 0;
      while (instances.hasNext()) {
        output.collect(id, instances.next());
        num++;
      }

      //assertEquals("Error! one key to many values! num: " + num, 1, num);
    }
  }

  public static class InstanceReducerNokey2 extends MapReduceBase implements
          Reducer<IntWritable, Text, NullWritable, Text> {

    public void reduce(IntWritable id, Iterator<Text> instances,
            OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
      int num = 0;
      while (instances.hasNext()) {
        output.collect(NullWritable.get(), instances.next());
        num++;
      }

      //assertEquals("Error! one key to many values! num: " + num, 1, num);
    }
  }
}