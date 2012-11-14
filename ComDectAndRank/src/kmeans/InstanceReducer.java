package kmeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;

public class InstanceReducer extends MapReduceBase implements
        Reducer<ClusterId, Instance, Text, Text> {

  public void reduce(ClusterId id, Iterator<Instance> instances,
          OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    Cluster cluresult = null;
    while (instances.hasNext()) {
      if (cluresult == null) {
        cluresult = new Cluster(instances.next());
      } else {
        cluresult.addtocluster(instances.next());
      }
    }
    output.collect(id.toText(), cluresult.toText());
  }
}
