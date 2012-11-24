package kmeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

//import static org.junit.Assert.*;

public class InstanceReduce2 {
  public static class InstanceReducer2 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {

    private boolean outdraw = false;

    public void configure(JobConf job) {
      // out put .NET format drawing data
      outdraw = Boolean.parseBoolean(job.get("outdraw"));
    }

    public void reduce(IntWritable id, Iterator<Text> instances,
            OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
      int num = 0;
      while (instances.hasNext()) {
          output.collect(id, instances.next());
      }

      // assertEquals("Error! one key to many values! num: " + num, 1, num);
    }
  }

  public static class InstanceReducerNokey2 extends MapReduceBase implements
          Reducer<IntWritable, Text, NullWritable, Text> {

    public void reduce(IntWritable id, Iterator<Text> instances,
            OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
      int num = 0;
      while (instances.hasNext()) {
        int assign = Integer.parseInt(instances.next().toString());
        output.collect(NullWritable.get(), new Text((assign + 1) + ""));
        num++;
      }

      // assertEquals("Error! one key to many values! num: " + num, 1, num);
    }
  }
}