package kmeans;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class InstanceMapper2 extends MapReduceBase implements
        Mapper<LongWritable, Text, IntWritable, Text> {

  Clusters clusters = new Clusters();

  private IntWritable outint = new IntWritable();

  public void configure(JobConf job) {
    try {
      Path[] clustersFiles = DistributedCache.getLocalCacheFiles(job);
      for (Path clustersFile : clustersFiles) {
        clusters.addclusters(clustersFile);
      }
    } catch (Exception ex) {
      ex.printStackTrace();
      System.err.println("Caught exception while parsing the cache file!");
    }
  }

  public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
          Reporter reporter) throws IOException {
    String line = value.toString();
    Instance instance = new Instance(line);
    double currentdis = Double.MAX_VALUE;
    double newdis;
    ClusterId idresult = null;
    try {
      Set<Map.Entry<ClusterId, Cluster>> entries = clusters.clusters.entrySet();
      for (Map.Entry<ClusterId, Cluster> en : entries) {
        if ((newdis = KmeansUtil.euclideandistance(en.getValue().data, instance.data)) < currentdis) {
          idresult = en.getKey();
          currentdis = newdis;
        }
      }

      outint.set(Integer.parseInt(instance.instanceId));
      output.collect(outint, idresult.toText());
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public static class OutPartitioner implements Partitioner<IntWritable, Text> {
    private int num_nodes = 0;
    
    @Override
    public void configure(JobConf job) {
      num_nodes = Integer.parseInt(job.get("num_nodes"));
    }

    @Override
    public int getPartition(IntWritable key, Text value, int numPartitions) {
      // order by ascending 
      // ceiling func
      return key.get() / ((num_nodes+numPartitions-1) / numPartitions);
    }
  }

}
