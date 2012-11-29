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
       extends Mapper<Object, Text, Text, Text>{
    
	  private final static Text degree = new Text("1");
	  private Text node1 = new Text();
	  private Text node2 = new Text();
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		  StringTokenizer itr = new StringTokenizer(value.toString());
		  String s1 = itr.nextToken();
		  String s2 = itr.nextToken();
		  if(s1.startsWith("#")) {}//error line
		  else{
			  node1.set(s1);
			  node2.set(s2);
			  if(itr.hasMoreTokens()) {
				  degree.set(itr.nextToken());
				  context.write(new Text(node1+"\t"+node2),new Text("v"+degree.toString()));
			  }
			  context.write(new Text(node1),degree);
			  //context.write(new Text(node2.toString()),one);
			  context.write(new Text(node1),new Text("v"+node2.toString()));
			  //context.write(new Text(node2.toString()),new IntWritable(-node1.get()));
		  }		  
	  }
  }
  
  public static class DegreeSumReducer 
       extends Reducer<Text,Text,Text,Text> {
	  public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		  List<Text> node = new ArrayList<Text>();
		  Text result = new Text("0");
		  StringTokenizer itr = new StringTokenizer(key.toString());
		  if(itr.countTokens()>1){
			  for (Text val : values) {
				  context.write(key, val);
			  }
			  
		  }else{
			  Double sum = 0.0;
			  for (Text val : values) {
				  if(val.toString().getBytes()[0]=='v'){
					  node.add(new Text(val.toString().substring(1)));
					  continue;
				  }else{
					  sum += Double.parseDouble(val.toString());
				  }
			  }
			  result.set(sum.toString());
			  //context.write(key,result);
			  for (Text val:node){
				  context.write(new Text(key.toString()+"\t"+(val.toString())), result);
				  context.write(new Text((val.toString())+"\t"+key.toString()), result);            
			  }
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
	  //job.setCombinerClass(DegreeSumReducer.class);
	  job.setReducerClass(DegreeSumReducer.class);
	  job.setMapOutputKeyClass(Text.class);
	  job.setMapOutputValueClass(Text.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


