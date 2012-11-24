package kmeans;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import static org.junit.Assert.*;

/**
 * InitVec: calculate the initial cluster center vector for kmeans
 * 
 * */
public class InitVec extends Configured implements Tool {

  private Path in_path, out_path;

  private int num_clusters, num_nodes, nreducers;

  //private static final Log LOG = LogFactory.getLog(InitVec.class);

  public static class InitVecMapper extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Instance> {

    private Random rand;

    private int num_clusters, num_nodes, curnum = 1, emitnum = 0;

    public void configure(JobConf job) {
      try {
        rand = new Random();
        num_clusters = Integer.parseInt(job.get("num_clusters"));
        num_nodes = Integer.parseInt(job.get("num_nodes"));

      } catch (Exception ex) {
        ex.printStackTrace();
        System.err.println("Caught exception while getting the configure params!");
      }
    }

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Instance> output,
            Reporter reporter) throws IOException {
      String line = value.toString();
      //LOG.info("initVec_line:" + line);

      Instance instance = new Instance(line);

      try {
        if (emitnum < num_clusters) {
          // ensure enough instances (>= num_clusters) emited
          if (curnum + (num_clusters - emitnum) >= num_nodes) {
            output.collect(new IntWritable(0), instance);
            emitnum++;
          }
          // emit only num_clusters instances (group to key==0)
          else if (rand.nextInt(num_nodes) < num_clusters) {
            output.collect(new IntWritable(0), instance);
            emitnum++;
          }
        }
        curnum++;
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }

  }

  public static class InitVecReducer extends MapReduceBase implements
          Reducer<IntWritable, Instance, Text, Text> {

    private int num_clusters, curnum = 0;

    public void configure(JobConf job) {
      num_clusters = Integer.parseInt(job.get("num_clusters"));
    }

    public void reduce(IntWritable key, Iterator<Instance> instances,
            OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      List<Cluster> ins = new ArrayList<Cluster>();

      while (instances.hasNext()) {
        ins.add(new Cluster(instances.next()));
        curnum++;
      }
      
      //assertTrue("Error! curnum "+curnum+" should >= num_clusters "+num_clusters, curnum >= num_clusters);
      for (int i = 0; i < ins.size() && i < num_clusters; i++) {
        output.collect(new Text(i + ""), ins.get(i).toText());
      }

    }
  }

  // Print the command-line usage text.
  protected static int printUsage() {
    System.out
            .println("Usage: InitVec <input_path> <output_path> <# of clusters> <# of nodes> <# of tasks> <taskid>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 6) {
      return printUsage();
    }
    
    String taskid = "_" + args[5];

    in_path = new Path(args[0] + taskid);
    out_path = new Path(args[1] + taskid);
    num_clusters = Integer.parseInt(args[2]);
    num_nodes = Integer.parseInt(args[3]);
    nreducers = Integer.parseInt(args[4]);

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(out_path))
      fs.delete(out_path, true);

    System.out.println("[Kmeans] Creating initial cluster centers.");
    JobClient.runJob(configInitvec());
    return 0;
  }

  private JobConf configInitvec() {
    final JobConf conf = new JobConf(getConf(), InitVec.class);
    conf.set("number_nodes", "" + num_nodes);
    conf.set("num_clusters", "" + num_clusters);
    conf.setJobName("KMeans_Stage0_InitVec");

    conf.setMapperClass(InitVecMapper.class);
    conf.setReducerClass(InitVecReducer.class);

    FileInputFormat.setInputPaths(conf, in_path);
    FileOutputFormat.setOutputPath(conf, out_path);

    conf.setNumReduceTasks(nreducers);

    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(Instance.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

  // Main entry point.
  public static void main(final String[] args) throws Exception {
    final int result = ToolRunner.run(new Configuration(), new InitVec(), args);

    System.exit(result);
  }

}
