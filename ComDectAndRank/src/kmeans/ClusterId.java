package kmeans;

import java.io.*;

import org.apache.hadoop.io.*;

public class ClusterId implements WritableComparable<ClusterId>{
	
	String clusterid;

	public ClusterId(){
		clusterid = null;
	}

	public ClusterId(String clusterid){
		this.clusterid = clusterid;
	}
	
	public int hashCode(){
		return clusterid.hashCode();
	}	

	public boolean equals(ClusterId other){
		return clusterid.equals(other.clusterid);
	}

	public void write(DataOutput out) throws IOException{
		out.writeUTF(clusterid);
	}	

	public void readFields(DataInput in) throws IOException{
		clusterid = in.readUTF();
	}
	
	public int compareTo(ClusterId other){		
			return clusterid.compareTo(other.clusterid);
	}

	public Text toText(){
		return new Text(clusterid);
	} 
}
