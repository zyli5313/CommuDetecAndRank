package spectral;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LaplaceGen {

  public static class LapMapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
	  private Text node1 = new Text();
	  private Text node2 = new Text();
	  private Text edge = new Text();
	  private String degree;
	  private DoubleWritable weight = new DoubleWritable();
	  public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		  StringTokenizer itr = new StringTokenizer(value.toString());
		  if(itr.countTokens()==2) {}
		  else{
			  node1.set(itr.nextToken());
			  node2.set(itr.nextToken());
			  edge.set(node1.toString()+"\t"+node2.toString());
			  degree = itr.nextToken();
			  if(degree.getBytes()[0]=='v') {
				  degree = degree.substring(1);
				  weight.set(Double.parseDouble(degree));
				  context.write(edge,new DoubleWritable(weight.get()));
			  }else{
				  weight.set(Double.parseDouble(degree));
				  context.write(edge,new DoubleWritable(1/Math.sqrt(weight.get())));
			  }		  
		  }
	  }
  }
  
  public static class LapReducer 
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	  private DoubleWritable result = new DoubleWritable();

	  public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		  double sum = 1;
		  for (DoubleWritable val : values) {
			  sum=sum*val.get();
		  }
		  result.set(sum);
		  context.write(key, result);
	  }
  }

  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  if (otherArgs.length != 2) {
		  System.err.println("Usage: wordcount <in> <out>");
		  System.exit(2);
	  }
	    Job job = new Job(conf, "Laplace Matrix Generator");
	    job.setJarByClass(LaplaceGen.class);
	    job.setMapperClass(LapMapper.class);
	    //job.setCombinerClass(LapReducer.class);
	    job.setReducerClass(LapReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


