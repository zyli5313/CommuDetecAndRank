package kmeans;

import java.util.Arrays;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class Cluster implements Writable {

  double cardinality;

  double[] data;

  public Cluster() {
  }

  public Cluster(Instance mean) {
    cardinality = mean.weight;
    data = Arrays.copyOf(mean.data, mean.data.length);
  }

  public Cluster(String s) {
    try {
      String[] ss = s.split(",");
      cardinality = Double.parseDouble(ss[0]);
      int len = Integer.parseInt(ss[1]);
      data = new double[len];
      for (int i = 0; i < len; i++)
        data[i] = Double.parseDouble(ss[i + 2]);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public void addtocluster(Instance instance) {
    if (data.length == instance.data.length) {
      double newcardinality = cardinality + instance.weight;
      for (int i = 0; i < data.length; i++) {
        data[i] = (data[i] * cardinality + instance.data[i] * instance.weight) / newcardinality;
      }
      cardinality = newcardinality;
    } else {
      System.err.println("Add to cluster with different dimensionality!");
    }
  }

  public void addtocluster(Cluster cluster) {
    if (data.length == cluster.data.length) {
      double newcardinality = cardinality + cluster.cardinality;
      for (int i = 0; i < data.length; i++) {
        data[i] = (data[i] * cardinality + cluster.data[i] * cluster.cardinality) / newcardinality;
      }
      cardinality = newcardinality;
    } else {
      System.err.println("Add to cluster with different dimensionality!");
    }
  }

  public void write(DataOutput out) throws IOException {
    String s = "";
    s = s + cardinality;
    s = s + "," + data.length;
    for (int i = 0; i < data.length; i++) {
      s = s + "," + data[i];
    }
    out.writeUTF(s);
  }

  public void readFields(DataInput in) throws IOException {
    String s = in.readUTF();
    String[] sdata = s.split(",");
    cardinality = Double.parseDouble(sdata[0]);
    int len = Integer.parseInt(sdata[1]);
    data = new double[len];
    for (int i = 0; i < len; i++) {
      data[i] = Double.parseDouble(sdata[i + 2]);
    }
  }

  // TODO: change input interface
  public Text toText() {
    String s = "";
    s = s + cardinality;
    s = s + "," + data.length;
    for (int i = 0; i < data.length; i++) {
      s = s + "," + data[i];
    }
    return new Text(s);
  }

}
