package spectral;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DegreeCount {

  public static class DegreeMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
	  private final static IntWritable one = new IntWritable(1);
	  private IntWritable node1 = new IntWritable();
	  private IntWritable node2 = new IntWritable();
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		  StringTokenizer itr = new StringTokenizer(value.toString());
		  node1.set(Integer.parseInt(itr.nextToken()));
		  node2.set(Integer.parseInt(itr.nextToken()));
		  context.write(new Text(node1.toString()),one);
		  context.write(new Text(node2.toString()),one);
		  context.write(new Text(node1.toString()),new IntWritable(-node2.get()));
		  //context.write(new Text(node2.toString()),new IntWritable(-node1.get()));
	  }
  }
  
  public static class DegreeSumReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		  List<IntWritable> node = new ArrayList<IntWritable>();
		  IntWritable result = new IntWritable(0);
		  int sum = 0;
		  for (IntWritable val : values) {
			  if(val.get()<0){
				  node.add(new IntWritable(-val.get()));
				  continue;
			  } 
			  if(val.get()>0)  sum += val.get();
		  }
		  result.set(sum);
		  context.write(key,result);
		  for (IntWritable val:node){
			  context.write(new Text(key.toString()+"\t"+(val.get())), result);
			  context.write(new Text((val.get())+"\t"+key.toString()), result);            
		  }
	  }
  }

  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  if (otherArgs.length != 2) {
		  System.err.println("Error!");
		  System.exit(2);
	  }
	  Job job = new Job(conf, "Degree Count");
	  job.setJarByClass(DegreeCount.class);
	  job.setMapperClass(DegreeMapper.class);
	  job.setCombinerClass(DegreeSumReducer.class);
	  job.setReducerClass(DegreeSumReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(IntWritable.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(IntWritable.class);
	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


