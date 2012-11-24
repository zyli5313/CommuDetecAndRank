/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: PageRankPrep.java
 - Convert the original edge file into column-normalized adjacency matrix format.
Version: 2.0
 ***********************************************************************/
package pic;

import java.io.*;

import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

//import static org.junit.Assert.*;

public class InitVec extends Configured implements Tool {
  // ////////////////////////////////////////////////////////////////////
  // STAGE 1: Compute initial vector for pic
  // - Input: edge file
  // - Output: partial row sum and partial matrix sum
  // ////////////////////////////////////////////////////////////////////
  public static class MapStage1 extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Text> {
    int make_symmetric = 0;

    private int num_nodes;

    public void configure(JobConf job) {
      make_symmetric = Integer.parseInt(job.get("make_symmetric"));
      num_nodes = Integer.parseInt(job.get("num_nodes"));

      System.out.println("MapStage1 : make_symmetric = " + make_symmetric);
    }

    public void map(final LongWritable key, final Text value,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      String line_text = value.toString();
      if (line_text.startsWith("#")) // ignore comments in edge file
        return;

      final String[] line = line_text.split("\t");
      if (line.length < 2) // ignore ill-formated data.
        return;
      
      int src_id = Integer.parseInt(line[0]);
      int dst_id = Integer.parseInt(line[1]);
      if (line.length == 3) {
        output.collect(new IntWritable(src_id), new Text(line[1] + "\t" + line[2]));
        // output every element of total matrix sum (key==num_nodes, which no one uses)
        output.collect(new IntWritable(num_nodes), new Text(line[2]));
      } else if (line.length == 2) {
        output.collect(new IntWritable(src_id), new Text(line[1] + "\t" + 1.0));
        // output every element of total matrix sum (key==num_nodes, which no one uses)
        output.collect(new IntWritable(num_nodes), new Text(1.0 + ""));
      }

      if (make_symmetric == 1) {
        if(line.length == 3) {
          output.collect(new IntWritable(dst_id), new Text(line[0] + "\t" + line[2]));
          output.collect(new IntWritable(num_nodes), new Text(line[2]));
        }
        else if(line.length == 2) {
          output.collect(new IntWritable(dst_id), new Text(line[0] + "\t" + 1.0));
          output.collect(new IntWritable(num_nodes), new Text(1.0 + ""));
        }
      }
    }
  }

  public static class RedStage1 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {
    private int num_nodes;

    private boolean start1 = false;

    public void configure(JobConf job) {
      num_nodes = Integer.parseInt(job.get("num_nodes"));
      start1 = Boolean.parseBoolean(job.get("start1"));
    }

    public void reduce(final IntWritable key, final Iterator<Text> values,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      ArrayList<Integer> dst_nodes_list = new ArrayList<Integer>();
      double rowtotal = 0.0, mattotal = 0.0;

      // special noe to compute total matirx sum
      if (key.get() == num_nodes) {
        while (values.hasNext()) {
          mattotal += Double.parseDouble(values.next().toString());
        }
        // for every node, output the total matrix sum
        int i = start1 ? 1 : 0;
        for (; i < num_nodes; i++)
          output.collect(new IntWritable(i), new Text("t" + mattotal));
      } else {
        while (values.hasNext()) {
          String[] cur_value_strs = values.next().toString().split("\t");
          // assertEquals("line error r1:"+cur_value_strs.toString(), 2, cur_value_strs.length);

          rowtotal += Double.parseDouble(cur_value_strs[1]);
        }

        // output row partial sum
        output.collect(key, new Text("r" + rowtotal));
      }
    }
  }

  // //////////////////////////////////////////////////////////////////////////////////////////////
  // STAGE 2: output initial vector
  // - Input: partial row sum and total matrix sum
  // - Output: initial vectors
  // //////////////////////////////////////////////////////////////////////////////////////////////
  public static class MapStage2 extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Text> {
    public void map(final LongWritable key, final Text value,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      final String[] line = value.toString().split("\t");

      output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
    }
  }

  public static class RedStage2 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(final IntWritable key, final Iterator<Text> values,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      double totalsum = 0.0, rowsum = 0.0;

      while (values.hasNext()) {
        String curval = values.next().toString();
        if (curval.startsWith("t"))
          totalsum = Double.parseDouble(curval.substring(1));
        else
          rowsum = Double.parseDouble(curval.substring(1));
      }

      output.collect(key, new Text("v" + rowsum / totalsum));
    }
  }

  // ////////////////////////////////////////////////////////////////////
  // command line interface
  // ////////////////////////////////////////////////////////////////////
  protected Path out_path = null;

  protected Path edge_path = null;

  protected Path tmp_path = null;

  protected int nreducers = 10;

  protected int make_symmetric = 0; // convert directed graph to undirected graph

  protected boolean start1 = false; // if data index starts from 1

  protected int num_nodes = 0;

  // Main entry point.
  public static void main(final String[] args) throws Exception {
    final int result = ToolRunner.run(new Configuration(), new InitVec(), args);

    System.exit(result);
  }

  // Print the command-line usage text.
  protected static int printUsage() {
    System.out
            .println("PicInitVec <edge_path> <output_path> <# of nodes> <# of reducers> <makesym or nosym> <start1 or start0>  <taskid>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  // submit the map/reduce job.
  public int run(final String[] args) throws Exception {
    if (args.length != 7) {
      System.out.println("args.length = " + args.length);
      int i;
      for (i = 0; i < args.length; i++) {
        System.out.println("args[" + i + "] = " + args[i]);
      }
      return printUsage();
    }

    
    edge_path = new Path(args[0]);
    tmp_path = new Path("./pic_tmpiv_"+args[6]);  //append taskid
    out_path = new Path(args[1]+"_"+args[6]);
    num_nodes = Integer.parseInt(args[2]);
    nreducers = Integer.parseInt(args[3]);
    if (args[4].compareTo("makesym") == 0)
      make_symmetric = 1;
    else
      make_symmetric = 0;

    if (args[5].compareTo("start1") == 0)
      start1 = true;
    else
      start1 = false;

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(out_path))
      fs.delete(out_path, true);
    if (fs.exists(tmp_path))
      fs.delete(tmp_path, true);

    System.out.println("\n-----===[PIC]===-----\n");
    System.out.println("[PIC] creating initial vector.\n");

    JobClient.runJob(configStage1());
    JobClient.runJob(configStage2());

    System.out.println("\n[PIC] creating init vec finished.");
    System.out.println("[PIC] init vec is saved in the HDFS " + args[1] + "\n");

    fs.delete(tmp_path);

    return 0;
  }

  // Configure pass1
  protected JobConf configStage1() throws Exception {
    final JobConf conf = new JobConf(getConf(), InitVec.class);
    conf.set("make_symmetric", "" + make_symmetric);
    conf.set("num_nodes", "" + num_nodes);
    conf.set("start1", "" + start1);
    conf.setJobName("PIC_InitVec1");

    conf.setMapperClass(MapStage1.class);
    conf.setReducerClass(RedStage1.class);

    FileInputFormat.setInputPaths(conf, edge_path);
    FileOutputFormat.setOutputPath(conf, tmp_path);

    conf.setNumReduceTasks(nreducers);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

  protected JobConf configStage2() throws Exception {
    final JobConf conf = new JobConf(getConf(), InitVec.class);
    conf.set("make_symmetric", "" + make_symmetric);
    conf.set("num_nodes", "" + num_nodes);
    conf.setJobName("PIC_InitVec2");

    conf.setMapperClass(MapStage2.class);
    conf.setReducerClass(RedStage2.class);

    FileInputFormat.setInputPaths(conf, tmp_path);
    FileOutputFormat.setOutputPath(conf, out_path);

    conf.setNumReduceTasks(nreducers);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }
}
