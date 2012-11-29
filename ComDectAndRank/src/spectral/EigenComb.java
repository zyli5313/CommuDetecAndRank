package spectral;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

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

public class EigenComb {

	  public static class EigenMapper 
	       extends Mapper<Object, Text, IntWritable, Text>{
	    
		  private IntWritable node = new IntWritable();
		  private Text eigen = new Text();
		  public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
			  StringTokenizer itr = new StringTokenizer(value.toString());
			  if(itr.hasMoreTokens()){
				  String s = "";
				  node.set(Integer.parseInt(itr.nextToken()));
				  while(itr.hasMoreTokens()){
					  s = s+"\t"+itr.nextToken();
				  }		
				  eigen.set(s.substring(1));
				  context.write(node,eigen);
			  }
		  }
	  }
	  
	  public static class EigenReducer 
	       extends Reducer<IntWritable,Text,IntWritable,Text> {
		  private Text result = new Text();

		  public void reduce(IntWritable key, Iterable<Text> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
			  if(key.get()!=0){
				  String eigenall = "";
				  List<String> eigen = new ArrayList<String>();
				  int i = 0;
				  String temp = null;
				  int flag = 0;
				  for (Text val : values) {
					  i++;
					  if(i==1) {
						  if(val.toString().charAt(0)=='v'){
							  eigenall = eigenall+"\t"+val.toString();
						  }else{
							  temp = val.toString();
							  flag = 1;
						  }
					  }else{
						  eigenall = eigenall+"\t"+val.toString();
					  }
				  }
				  if(flag == 1) eigenall = eigenall+"\t"+"v"+temp;
				  flag = 0;
				  i = 0;
				  eigenall = eigenall.substring(1);
				  result.set(eigenall);
				  context.write(key, result);
			  }			  
		  }
	  }

	  public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		  if (otherArgs.length != 3) {
			  System.err.println("Error!");
			  System.exit(3);
		  }
		    Job job = new Job(conf, "Eigen Vectors Combine");
		    job.setJarByClass(EigenComb.class);
		    job.setMapperClass(EigenMapper.class);
		    //job.setCombinerClass(EigenReducer.class);
		    job.setReducerClass(EigenReducer.class);
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
	}