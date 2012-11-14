/**
 *  Kmeans algorithm implemented in map-reduce framework using Hadoop.
 *
 */

package kmeans;

import java.io.*;
import java.util.*;
import java.net.URI;

import kmeans.InstanceMapper2.OutPartitioner;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Kmeans extends Configured implements Tool {
  
  private int num_clusters, number_nodes, nreducers;
  private Path init_path, in_path, out_path;

  private Kmeans() {
  } // singleton

  public int run(String[] args) {
    if (args.length != 6) {
      /*
       * System.out.println(args.length); for (String s: args) System.out.println(s);
       */
      System.out.println("Usage: Kmeans <in> <out> <init> <# of clusters> <# of nodes> <# reducers>");
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    try {

      FileSystem fs = FileSystem.get(getConf());

      Clusters oldclusters = null;
      Clusters newclusters = null;

      in_path = new Path(args[0]);
      out_path = new Path(args[1]);
      init_path = new Path(args[2]);
      num_clusters = Integer.parseInt(args[3]);
      number_nodes = Integer.parseInt(args[4]);
      nreducers = Integer.parseInt(args[5]);
      double error = ((double) 0.01 / (double) number_nodes);

      /*System.out.println(in_path.toString());
      System.out.println(out_path.toString());
      System.out.println(init_path.toString());
      System.out.println(error);
       */
      if(fs.exists(out_path))
        fs.delete(out_path, true);
      
      // generate initial cluster: randomly choose k vectors from input
      
      int numiter = 0;
      do {
        System.out.println("-----Kmeans-----\niteration:" + (++numiter) + "\n");
        
        oldclusters = newclusters;

        JobConf conf = new JobConf(getConf(), Kmeans.class);
        conf.setJobName("kmeans");
        URI[] cachefiles = getFiles(init_path, fs);
        for (URI uri : cachefiles) {
          DistributedCache.addCacheFile(uri, conf);
        }

        conf.setMapOutputKeyClass(ClusterId.class);
        conf.setMapOutputValueClass(Instance.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(InstanceMapper.class);
        conf.setCombinerClass(InstanceCombiner.class);
        conf.setReducerClass(InstanceReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        //conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, in_path);
        FileOutputFormat.setOutputPath(conf, out_path);
        
        conf.setNumReduceTasks(nreducers);

        JobClient.runJob(conf);
        fs.delete(init_path, true);
        fs.rename(out_path, init_path);

        newclusters = new Clusters(init_path, fs);

      } while (newclusters.isequals(oldclusters, error) == false);

      JobConf conf = new JobConf(getConf(), Kmeans.class);
      conf.setJobName("kmeans_result");
      URI[] cachefiles = getFiles(init_path, fs);
      for (URI uri : cachefiles) {
        DistributedCache.addCacheFile(uri, conf);
      }

      conf.setMapOutputKeyClass(IntWritable.class);
      conf.setMapOutputValueClass(Text.class);

      conf.setMapperClass(InstanceMapper2.class);
      conf.setNumReduceTasks(0);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
      conf.setOutputKeyComparatorClass(IntWritable.Comparator.class);
      conf.setPartitionerClass(OutPartitioner.class);

      FileInputFormat.setInputPaths(conf, in_path);
      FileOutputFormat.setOutputPath(conf, out_path);

      JobClient.runJob(conf);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Kmeans Program Failed!");
    }
    return 0;
  }
  
  URI[] getFiles(Path path, FileSystem fs) throws Exception {

    FileStatus[] files = fs.listStatus(path);
    ArrayList<URI> urifiles = new ArrayList<URI>();
    for (int i = 0; i < files.length; i++) {
      Path p = files[i].getPath();
      if (p.getName().startsWith("part")) {
        URI uri = p.toUri();
        urifiles.add(uri);
      }

    }
    URI[] uris = new URI[urifiles.size()];
    urifiles.toArray(uris);
    return uris;

  }
  

  public static void main(String[] args) {
    try {
      int result = ToolRunner.run(new Configuration(), new Kmeans(), args);
      System.exit(result);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}
