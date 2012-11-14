package kmeans;

import java.util.Arrays;
import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
//import static org.junit.Assert.*;

public class Instance implements Writable {

  String instanceId;
  double[] data;
  double weight;
  
  private static final Log LOG = LogFactory.getLog(InitVec.class);

  public Instance() {
  }

  public Instance(String s) {
    String[] ss = s.split(",");
    instanceId = ss[0];
    weight = Double.parseDouble(ss[1]);
    data = new double[Integer.parseInt(ss[2])];
    //assertEquals("data len error:"+ss[0]+ss[1]+ss[2], 1, data.length);
    
    for (int i = 0; i < data.length; i++) {
      data[i] = Double.parseDouble(ss[i + 3]);
    }
  }

  public Instance(Instance instance) {
    instanceId = instance.instanceId;
    data = Arrays.copyOf(instance.data, instance.data.length);
    weight = instance.weight;
  }

  public void combine(Instance instance) {
    if (data.length == instance.data.length) {
      double newweight = weight + instance.weight;
      for (int i = 0; i < data.length; i++) {
        data[i] = (data[i] * weight + instance.data[i] * instance.weight) / newweight;
      }
      weight = newweight;
    } else {
      System.err.println("Combine instance with different dimensionality!");
    }
  }

  public void write(DataOutput out) throws IOException {
    if (data != null) {
      out.writeUTF(instanceId);
      out.writeDouble(weight);
      out.writeInt(data.length);
      for (int i = 0; i < data.length; i++) {
        out.writeDouble(data[i]);
      }
    }
  }

  public void readFields(DataInput in) throws IOException {
    instanceId = in.readUTF();
    weight = in.readDouble();
    data = new double[in.readInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = in.readDouble();
    }
  }
}
