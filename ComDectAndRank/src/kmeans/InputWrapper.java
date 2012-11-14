package kmeans;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

/**
 * InputWrapper: separate stage for kemans input wrapper. (no reduce task) 
 * i.e. change input type to feed kmeans input type
 * 
 * */
public class InputWrapper extends Configured implements Tool {

  private Path in_path, out_path;

  private int nreducers;

  public static class WrapperMapper extends MapReduceBase implements
          Mapper<LongWritable, Text, Text, NullWritable> {
    
    public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output,
            Reporter reporter) throws IOException {
      String line = value.toString();
      String[] strs = line.split("\t");
      // TODO: Read input here
      StringBuilder sb = new StringBuilder();
      
      // append id
      sb.append(strs[0]);
      sb.append(",1");  // weight
      sb.append("," + (strs.length-1)); // data array length
      for(int i = 1; i < strs.length; i++) {
        if(strs[i].startsWith("v"))
          sb.append("," + strs[i].substring(1));
        else
          sb.append("," + strs[i]);
      }
      
      output.collect(new Text(sb.toString()), NullWritable.get());
    }

  }

  // Print the command-line usage text.
  protected static int printUsage() {
    System.out
            .println("Usage: InputWrapper <input_path> <output_path> <# of tasks>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      return printUsage();
    }

    in_path = new Path(args[0]);
    out_path = new Path(args[1]);
    nreducers = Integer.parseInt(args[2]);
    
    FileSystem fs = FileSystem.get(getConf());
    if(fs.exists(out_path))
      fs.delete(out_path, true);

    System.out.println("[Kmeans] Input Wrapper for kmeans.");
    JobClient.runJob(configWrapper());
    return 0;
  }

  private JobConf configWrapper() {
    final JobConf conf = new JobConf(getConf(), InputWrapper.class);
    conf.setJobName("KMeans_InputWrapper");

    conf.setMapperClass(WrapperMapper.class);
    conf.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(conf, in_path);
    FileOutputFormat.setOutputPath(conf, out_path);

    conf.setNumReduceTasks(nreducers);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    return conf;
  }
  
  // Main entry point.
  public static void main(final String[] args) throws Exception {
    final int result = ToolRunner.run(new Configuration(), new InputWrapper(), args);

    System.exit(result);
  }

}
