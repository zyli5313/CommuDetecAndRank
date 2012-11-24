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
File: PageRankNaive.java
 - PageRank using plain matrix-vector multiplication.
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

class MinMaxInfo {
  public double min;

  public double max;
};

public class Pic extends Configured implements Tool {
  protected static enum PrCounters {
    CONVERGE_CHECK
  }

  protected static double converge_threshold = 0.000001;

  // ////////////////////////////////////////////////////////////////////
  // STAGE 1: Generate partial matrix-vector multiplication results.
  // Perform hash join using Vector.rowid == Matrix.colid.
  // - Input: row-normalized affinity matrix W, vector vt
  // - Output: partial matrix-vector multiplication results.
  // ////////////////////////////////////////////////////////////////////
  public static class MapStage1 extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Text> {
    int make_symmetric = 0;

    public void configure(JobConf job) {
      make_symmetric = Integer.parseInt(job.get("make_symmetric"));

      System.out.println("MapStage1 : make_symmetric = " + make_symmetric);
    }

    public void map(final LongWritable key, final Text value,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      String line_text = value.toString();
      if (line_text.startsWith("#")) // ignore comments in edge file
        return;

      final String[] line = line_text.split("\t");

      if (line[1].charAt(0) == 'v') { // vector : ROWID VALUE('vNNNN')
        output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
      } else {
        // In matrix-vector multiplication, we output (dst, src) here
        // since vector(i) need to be multiplied by W[:][i]
        int src_id = Integer.parseInt(line[0]);
        int dst_id = Integer.parseInt(line[1]);
        output.collect(new IntWritable(dst_id), new Text(line[0] + "\t" + line[2]));

        if (make_symmetric == 1)
          output.collect(new IntWritable(src_id), new Text(line[1] + "\t" + line[2]));

      }
    }
  }

  public static class RedStage1 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {
    int number_nodes = 0;

    public void configure(JobConf job) {
      number_nodes = Integer.parseInt(job.get("number_nodes"));

      System.out.println("RedStage1: number_nodes = " + number_nodes);
    }

    public void reduce(final IntWritable key, final Iterator<Text> values,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      int i;
      double vt = 0;

      ArrayList<Integer> src_nodes_list = new ArrayList<Integer>();
      ArrayList<Double> weightlist = new ArrayList<Double>();

      while (values.hasNext()) {
        String line_text = values.next().toString();
        final String[] line = line_text.split("\t");

        if (line.length == 1) {
          if (line_text.charAt(0) == 'v') // vector : VALUE
            vt = Double.parseDouble(line_text.substring(1));
          output.collect(key, new Text("s" + vt));
        } else { // edge : src \t weight
          // assertEquals("line error r1:"+line_text, 2, line.length);

          // src \t weight
          src_nodes_list.add(Integer.parseInt(line[0]));
          weightlist.add(Double.parseDouble(line[1]));
        }
      }

      // TODO: mv into the loop
      // output.collect(key, new Text("s" + vt));

      int indeg = src_nodes_list.size();
      double totalDstWeight = 0.0;

      for (i = 0; i < indeg; i++) {
        double wij = vt * weightlist.get(i);
        totalDstWeight += wij;
        output.collect(new IntWritable(src_nodes_list.get(i)), new Text("v" + wij));
      }

      // prepare for adding up all elements in W*v (key==number_nodes)
      output.collect(new IntWritable(number_nodes), new Text("t" + totalDstWeight));

      // add up weight by column (sum up dst combine2) to become a row vector
      /*
       * for (i = 0; i < indeg; i++) { output.collect(new IntWritable(src_nodes_list.get(i)), new
       * Text("t" + totalDstWeight)); }
       */
    }
  }

  // //////////////////////////////////////////////////////////////////////////////////////////////
  // STAGE 2: compute normalization sum (|W*v|L1)
  // - Input: partial multiplication results
  // - Output: normalization number and partial multiplication results
  // //////////////////////////////////////////////////////////////////////////////////////////////
  public static class MapStage2 extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Text> {
    // Identity mapper
    public void map(final LongWritable key, final Text value,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      final String[] line = value.toString().split("\t");
      // assertEquals("line error m2:"+value.toString(), 2, line.length);

      output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
    }
  }

  public static class RedStage2 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {
    private int number_nodes = 0;

    private boolean start1 = false;

    public void configure(JobConf job) {
      number_nodes = Integer.parseInt(job.get("number_nodes"));
      start1 = Boolean.parseBoolean(job.get("start1"));
    }

    public void reduce(final IntWritable key, final Iterator<Text> values,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      double l1norm = 0.0;

      // compute total sum (l1norm)
      // TODO: ERROR here!
      if (key.get() == number_nodes) { // key==number_nodes does not exist actually
        while (values.hasNext()) {
          l1norm += Double.parseDouble(values.next().toString().substring(1));

          //System.out.println("l1norm (summing up) " + l1norm);
        }
        // data index starts from 1
        int i = start1 ? 1 : 0;
        System.out.println("l1norm (output) " + l1norm);

        for (; i < number_nodes; i++)
          output.collect(new IntWritable(i), new Text("t" + l1norm));
      }
      // others output as normal
      else {
        while (values.hasNext())
          output.collect(key, values.next());
      }
    }
  }
  
  // //////////////////////////////////////////////////////////////////////////////////////////////
  // STAGE 3: merge multiplication results.
  // - Input: partial multiplication results
  // - Output: combined multiplication results
  // //////////////////////////////////////////////////////////////////////////////////////////////
  public static class MapStage3 extends MapReduceBase implements
          Mapper<LongWritable, Text, IntWritable, Text> {
    // Identity mapper
    public void map(final LongWritable key, final Text value,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      final String[] line = value.toString().split("\t");
      // assertEquals("line error m2:"+value.toString(), 2, line.length);

      output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
    }
  }

  public static class RedStage3 extends MapReduceBase implements
          Reducer<IntWritable, Text, IntWritable, Text> {
    int number_nodes = 0;

    double converge_threshold = 0;

    int change_reported = 0;

    private FileSystem fs;
    private Path prediff_path;
    //private Path prediff_path = new Path("./pic_prediff/part-00000");

    public void configure(JobConf job) {
      number_nodes = Integer.parseInt(job.get("number_nodes"));
      converge_threshold = Double.parseDouble(job.get("converge_threshold"));
      prediff_path = new Path(job.get("prediff_path"));
      try {
        fs = FileSystem.get(job);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      try {
        // initialize prediff not exist
        if (fs.exists(prediff_path))
          fs.delete(prediff_path, true);

        System.out.println("RedStage2: number_nodes = " + number_nodes + ", converge_threshold = "
                + converge_threshold);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    public void reduce(final IntWritable key, final Iterator<Text> values,
            final OutputCollector<IntWritable, Text> output, final Reporter reporter)
            throws IOException {
      double vtnext = 0.0;
      double vtpre = 0.0;
      double vtsum = 0.0; // used in normalization

      while (values.hasNext()) {
        String cur_value_str = values.next().toString();
        if (cur_value_str.charAt(0) == 's')
          vtpre = Double.parseDouble(cur_value_str.substring(1));
        else if (cur_value_str.charAt(0) == 'v')
          vtnext += Double.parseDouble(cur_value_str.substring(1));
        else { // normalization number
          vtsum = Double.parseDouble(cur_value_str.substring(1));
        }
      }

      // normalize
      // TODO: do not normalize
      vtnext /= vtsum;
      output.collect(key, new Text("v" + vtnext));
      System.out.println(String.format("vtpre %f\tvtnext %f\t vtsum %f", vtpre, vtnext, vtsum));
      
      // store only once for every iteration
      // TODO: convergence check is not as what as paper said
      if (change_reported == 0) {
        double diff = Math.abs(vtpre - vtnext);

        System.out.println("diff:" + diff + "\tconverge_threshold:"
                + converge_threshold);

        if (diff > converge_threshold) {
          reporter.incrCounter(PrCounters.CONVERGE_CHECK, 1);
          change_reported = 1;
        }
      }
      /*if (change_reported == 0) {
        double diff = Math.abs(vtpre - vtnext);

        double prediff = 0.0;
        if (fs.exists(prediff_path)) {
          FSDataInputStream is = fs.open(prediff_path);
          prediff = is.readDouble();
          is.close();
        }
        System.out.println("prediff:" + prediff + "\tdiff:" + diff + "\tconverge_threshold:"
                + converge_threshold);

        if (Math.abs(prediff - diff) > converge_threshold) {
          reporter.incrCounter(PrCounters.CONVERGE_CHECK, 1);
          change_reported = 1;

          // save diff
          if (fs.exists(prediff_path))
            fs.delete(prediff_path);
          FSDataOutputStream os = fs.create(prediff_path, true);
          os.writeDouble(diff);
          os.close();
        }
      }*/
    }
  }

  /*
   * // //////////////////////////////////////////////////////////////////// // STAGE 3: After
   * finding pagerank, calculate min/max pagerank // - Input: The converged PageRank vector // -
   * Output: (key 0) minimum PageRank, (key 1) maximum PageRank //
   * //////////////////////////////////////////////////////////////////// public static class
   * MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable,
   * DoubleWritable> { private final IntWritable from_node_int = new IntWritable();
   * 
   * public void map(final LongWritable key, final Text value, final OutputCollector<IntWritable,
   * DoubleWritable> output, final Reporter reporter) throws IOException { String line_text =
   * value.toString(); if (line_text.startsWith("#")) // ignore comments in edge file return;
   * 
   * final String[] line = line_text.split("\t"); double pagerank =
   * Double.parseDouble(line[1].substring(1)); output.collect(new IntWritable(0), new
   * DoubleWritable(pagerank)); output.collect(new IntWritable(1), new DoubleWritable(pagerank)); }
   * }
   * 
   * public static class RedStage3 extends MapReduceBase implements Reducer<IntWritable,
   * DoubleWritable, IntWritable, DoubleWritable> { int number_nodes = 0;
   * 
   * double mixing_c = 0;
   * 
   * double random_coeff = 0;
   * 
   * public void configure(JobConf job) { number_nodes = Integer.parseInt(job.get("number_nodes"));
   * mixing_c = Double.parseDouble(job.get("mixing_c")); random_coeff = (1 - mixing_c) / (double)
   * number_nodes;
   * 
   * System.out.println("RedStage2: number_nodes = " + number_nodes + ", mixing_c = " + mixing_c +
   * ", random_coeff = " + random_coeff); }
   * 
   * public void reduce(final IntWritable key, final Iterator<DoubleWritable> values, final
   * OutputCollector<IntWritable, DoubleWritable> output, final Reporter reporter) throws
   * IOException { int i; double min_value = 1.0; double max_value = 0.0;
   * 
   * int min_or_max = key.get(); // 0 : min, 1: max
   * 
   * while (values.hasNext()) { double cur_value = values.next().get();
   * 
   * if (min_or_max == 0) { // find min if (cur_value < min_value) min_value = cur_value; } else {
   * // find max if (cur_value > max_value) max_value = cur_value; } }
   * 
   * if (min_or_max == 0) output.collect(key, new DoubleWritable(min_value)); else
   * output.collect(key, new DoubleWritable(max_value)); } }
   * 
   * // //////////////////////////////////////////////////////////////////// // STAGE 4 : Find
   * distribution of pageranks. // - Input: The converged PageRank vector // - Output: The histogram
   * of PageRank vector in 1000 bins between min_PageRank and max_PageRank //
   * //////////////////////////////////////////////////////////////////// public static class
   * MapStage4 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
   * { private final IntWritable from_node_int = new IntWritable();
   * 
   * double min_pr = 0;
   * 
   * double max_pr = 0;
   * 
   * double gap_pr = 0;
   * 
   * int hist_width = 1000;
   * 
   * public void configure(JobConf job) { min_pr = Double.parseDouble(job.get("min_pr")); max_pr =
   * Double.parseDouble(job.get("max_pr")); gap_pr = max_pr - min_pr;
   * 
   * System.out.println("MapStage4: min_pr = " + min_pr + ", max_pr = " + max_pr); }
   * 
   * public void map(final LongWritable key, final Text value, final OutputCollector<IntWritable,
   * IntWritable> output, final Reporter reporter) throws IOException { String line_text =
   * value.toString(); if (line_text.startsWith("#")) // ignore comments in edge file return;
   * 
   * final String[] line = line_text.split("\t"); double pagerank =
   * Double.parseDouble(line[1].substring(1)); int distr_index = (int) (hist_width * (pagerank -
   * min_pr) / gap_pr) + 1; if (distr_index == hist_width + 1) distr_index = hist_width;
   * output.collect(new IntWritable(distr_index), new IntWritable(1)); } }
   * 
   * public static class RedStage4 extends MapReduceBase implements Reducer<IntWritable,
   * IntWritable, IntWritable, IntWritable> { public void reduce(final IntWritable key, final
   * Iterator<IntWritable> values, final OutputCollector<IntWritable, IntWritable> output, final
   * Reporter reporter) throws IOException { int sum = 0;
   * 
   * while (values.hasNext()) { int cur_value = values.next().get(); sum += cur_value; }
   * 
   * output.collect(key, new IntWritable(sum)); } }
   */
  // ////////////////////////////////////////////////////////////////////
  // command line interface
  // ////////////////////////////////////////////////////////////////////
  protected Path W_path = null;

  protected Path finalout_path = null;

  protected Path tempmv_path = null;

  protected Path output_path = null;

  protected Path prediff_path = null, tempmv2_path = null;

  protected String local_output_path;

  // protected Path minmax_path = new Path("pr_minmax");
  //
  // protected Path distr_path = new Path("pr_distr");

  protected double thresbase = 0.001;
  protected int number_nodes = 0;

  protected int niteration = 32;

  protected int nreducers = 1;

  protected int make_symmetric = 0; // convert directed graph to undirected graph

  protected boolean start1 = false; // data idx starts from 1?

  // Main entry point.
  public static void main(final String[] args) throws Exception {
    final int result = ToolRunner.run(new Configuration(), new Pic(), args);

    System.exit(result);
  }

  // Print the command-line usage text.
  protected static int printUsage() {
    System.out
            .println("Pic <edge_path> <base threshold> <output_path> <# of nodes>  <# of tasks> <max iteration> <makesym or nosym> <new or contNN> <start0 or start1> <taskid>");

    ToolRunner.printGenericCommandUsage(System.out);

    return -1;
  }

  // submit the map/reduce job.
  public int run(final String[] args) throws Exception {
    if (args.length != 10) {
      return printUsage();
    }

    int i;
    BufferedWriter out = new BufferedWriter(new FileWriter("./time_"+args[9]+".log"));
    W_path = new Path(args[0]+"_"+args[9]);
    finalout_path = new Path("./pic_out_"+args[9]); //append taskid
    prediff_path = new Path("./pic_prediff_"+args[9]);
    thresbase = Double.parseDouble(args[1]);
    tempmv_path = new Path("./tempmv_path_"+args[9]);
    tempmv2_path = new Path("./tempmv2_path_"+args[9]);
    output_path = new Path(args[2]+"_"+args[9]);
    number_nodes = Integer.parseInt(args[3]);
    nreducers = Integer.parseInt(args[4]);
    niteration = Integer.parseInt(args[5]);

    if (args[6].compareTo("makesym") == 0)
      make_symmetric = 1;
    else
      make_symmetric = 0;

    int cur_iteration = 1;
    if (args[7].startsWith("cont"))
      cur_iteration = Integer.parseInt(args[7].substring(4));

    if (args[8].compareTo("start0") == 0)
      start1 = false;
    else
      start1 = true;

    FileSystem fs = FileSystem.get(getConf());
    if (fs.exists(output_path))
      fs.delete(output_path, true);
    if (fs.exists(tempmv_path))
      fs.delete(tempmv_path, true);
    if (fs.exists(tempmv2_path))
      fs.delete(tempmv2_path, true);

    local_output_path = args[2] + "_temp";

    converge_threshold = (thresbase / (double) number_nodes);

    System.out.println("\n-----===[PIC]===-----\n");

    long startTime = System.currentTimeMillis();
//    if (cur_iteration == 1)
//      gen_initial_vector(number_nodes, finalout_path, start1);

    // Run pagerank until converges.
    for (i = cur_iteration; i <= niteration; i++) {
      System.out.println("[PIC] Computing Pic. Max iteration = " + niteration + ", threshold = "
              + converge_threshold + ", cur_iteration=" + i + "\n");
      out.write(String.format("iter:%d\ttime:%d\n", i, (System.currentTimeMillis()-startTime)/1000));
      
      JobClient.runJob(configStage1());
      JobClient.runJob(configStage2());
      RunningJob job = JobClient.runJob(configStage3());

      // The counter is newly created per every iteration.
      Counters c = job.getCounters();
      long changed = c.getCounter(PrCounters.CONVERGE_CHECK);
      System.out.println("[PIC] Iteration = " + i + ", changed reducer = " + changed);

      if (changed == 0) {
        System.out.println("Pic vector converged. Now preparing to finish...");
        fs.delete(finalout_path);
        fs.delete(tempmv_path);
        fs.delete(tempmv2_path);
        fs.rename(output_path, finalout_path); // src, dst
        break;
      }

      // rotate directory
      fs.delete(finalout_path);
      fs.delete(tempmv_path);
      fs.delete(tempmv2_path);
      fs.rename(output_path, finalout_path);
    }

    if (i == niteration) {
      System.out.println("Reached the max iteration. Now preparing to finish...");
    }

    /*
     * // find min/max of pageranks System.out.println("Finding minimum and maximum pageranks...");
     * JobClient.runJob(configStage3());
     * 
     * FileUtil.fullyDelete(FileSystem.getLocal(getConf()), new Path(local_output_path)); String
     * new_path = local_output_path + "/"; fs.copyToLocalFile(minmax_path, new Path(new_path));
     * 
     * MinMaxInfo mmi = readMinMax(new_path); System.out.println("min = " + mmi.min + ", max = " +
     * mmi.max);
     * 
     * // find distribution of pageranks JobClient.runJob(configStage4(mmi.min, mmi.max));
     */
    long endTime = System.currentTimeMillis();
    long elapse = (endTime - startTime) / 1000;
    long h = elapse / 3600;
    long m = (elapse-h*3600) / 60;
    long s = elapse - h*3600 - m*60;
    fs.delete(tempmv2_path);
    System.out.println("\n[PIC] Pic computed.");
    System.out.println("[PIC] The final Pic are in the HDFS ./pic_out.");
    System.out.println(String.format("[PIC] total time runing PIC: %d secs (%d h %d m %d s)", elapse, h, m, s));
    System.out.println(String.format("[PIC] total number of iteration: %d", i));
    
    out.write(String.format("[PIC] total time runing PIC: %d secs (%d h %d m %d s)\n", elapse, h, m, s));
    out.write(String.format("[PIC] total number of iteration: %d", i));
    out.close();
    
    return 0;
  }

  // generate initial pic vector
  // TODO: id is not start from 0 and is not consecutive
  public void gen_initial_vector(int number_nodes, Path vector_path, boolean start1)
          throws IOException {
    int i, j = 0;
    int milestone = number_nodes / 10;
    String file_name = "pic_init_vector.temp";
    FileWriter file = new FileWriter(file_name);
    BufferedWriter out = new BufferedWriter(file);

    System.out.print("Creating initial pic vectors...");
    double initial_rank;
    if (!start1)
      initial_rank = 1.0 / (double) number_nodes;
    else
      initial_rank = 1.0 / (double) (number_nodes - 1);

    // TODO: matlab label starts from 1
    i = start1 ? 1 : 0;
    for (; i < number_nodes; i++) {
      out.write(i + "\tv" + initial_rank + "\n");
      if (++j > milestone) {
        System.out.print(".");
        j = 0;
      }
    }
    out.close();
    System.out.println("");

    // copy it to curbm_path, and delete temporary local file.
    final FileSystem fs = FileSystem.get(getConf());
    fs.copyFromLocalFile(true, new Path("./" + file_name), new Path(vector_path.toString() + "/"
            + file_name));
  }

  // read neighborhood number after each iteration.
  public static MinMaxInfo readMinMax(String new_path) throws Exception {
    MinMaxInfo info = new MinMaxInfo();
    String output_path = new_path + "/part-00000";
    String file_line = "";

    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(
              new FileInputStream(output_path), "UTF8"));

      // Read first line
      file_line = in.readLine();

      // Read through file one line at time. Print line # and line
      while (file_line != null) {
        final String[] line = file_line.split("\t");

        if (line[0].startsWith("0"))
          info.min = Double.parseDouble(line[1]);
        else
          info.max = Double.parseDouble(line[1]);

        file_line = in.readLine();
      }

      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return info;// result;
  }

  // Configure pass1
  protected JobConf configStage1() throws Exception {
    final JobConf conf = new JobConf(getConf(), Pic.class);
    conf.set("number_nodes", "" + number_nodes);
    conf.set("make_symmetric", "" + make_symmetric);
    conf.setJobName("Pic_Stage1");

    conf.setMapperClass(MapStage1.class);
    conf.setReducerClass(RedStage1.class);

    // first time finalout_path contains the initial vector
    FileInputFormat.setInputPaths(conf, W_path, finalout_path);
    FileOutputFormat.setOutputPath(conf, tempmv2_path);

    conf.setNumReduceTasks(nreducers);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

  // Configure pass1
  protected JobConf configStage2() throws Exception {
    final JobConf conf = new JobConf(getConf(), Pic.class);
    conf.set("number_nodes", "" + number_nodes);
    conf.set("start1", "" + start1);
    conf.setJobName("Pic_Stage2");

    conf.setMapperClass(MapStage2.class);
    conf.setReducerClass(RedStage2.class);

    // TODO: wrong path
    FileInputFormat.setInputPaths(conf, tempmv2_path);
    FileOutputFormat.setOutputPath(conf, tempmv_path);

    conf.setNumReduceTasks(nreducers);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

  // Configure pass2
  protected JobConf configStage3() throws Exception {
    final JobConf conf = new JobConf(getConf(), Pic.class);
    conf.set("number_nodes", "" + number_nodes);
    conf.set("converge_threshold", "" + converge_threshold);
    conf.set("prediff_path", "" + prediff_path);
    conf.setJobName("Pic_Stage3");

    conf.setMapperClass(MapStage3.class);
    conf.setReducerClass(RedStage3.class);

    FileInputFormat.setInputPaths(conf, tempmv_path);
    FileOutputFormat.setOutputPath(conf, output_path); // per iteration output

    conf.setNumReduceTasks(nreducers);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(Text.class);

    return conf;
  }

  /*
   * // Configure pass3 protected JobConf configStage3() throws Exception { final JobConf conf = new
   * JobConf(getConf(), Pic.class); conf.set("number_nodes", "" + number_nodes);
   * conf.set("mixing_c", "" + mixing_c); conf.set("converge_threshold", "" + converge_threshold);
   * conf.setJobName("Pagerank_Stage3");
   * 
   * conf.setMapperClass(MapStage3.class); conf.setReducerClass(RedStage3.class);
   * conf.setCombinerClass(RedStage3.class);
   * 
   * FileInputFormat.setInputPaths(conf, vector_path); FileOutputFormat.setOutputPath(conf,
   * minmax_path);
   * 
   * conf.setNumReduceTasks(1);
   * 
   * conf.setOutputKeyClass(IntWritable.class); conf.setOutputValueClass(DoubleWritable.class);
   * 
   * return conf; }
   * 
   * // Configure pass4 protected JobConf configStage4(double min_pr, double max_pr) throws
   * Exception { final JobConf conf = new JobConf(getConf(), Pic.class); conf.set("min_pr", "" +
   * min_pr); conf.set("max_pr", "" + max_pr);
   * 
   * conf.setJobName("Pagerank_Stage4");
   * 
   * conf.setMapperClass(MapStage4.class); conf.setReducerClass(RedStage4.class);
   * conf.setCombinerClass(RedStage4.class);
   * 
   * FileInputFormat.setInputPaths(conf, vector_path); FileOutputFormat.setOutputPath(conf,
   * distr_path);
   * 
   * conf.setNumReduceTasks(nreducers);
   * 
   * conf.setOutputKeyClass(IntWritable.class); conf.setOutputValueClass(IntWritable.class);
   * 
   * return conf; }
   */
}
