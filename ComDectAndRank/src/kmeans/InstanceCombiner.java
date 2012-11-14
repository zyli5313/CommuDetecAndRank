package kmeans;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

public class InstanceCombiner extends MapReduceBase implements
        Reducer<ClusterId, Instance, ClusterId, Instance> {

  public void reduce(ClusterId id, Iterator<Instance> instances,
          OutputCollector<ClusterId, Instance> output, Reporter reporter) throws IOException {
    Instance insresult = null;
    while (instances.hasNext()) {
      if (insresult == null) {
        insresult = new Instance(instances.next());
      } else {
        insresult.combine(instances.next());
      }
    }
    output.collect(id, insresult);
  }
}
