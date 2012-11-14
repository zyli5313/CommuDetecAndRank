package kmeans;

import java.util.Hashtable;
import java.io.*;
import java.net.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class Clusters{

	Hashtable<ClusterId, Cluster> clusters;
	
	public boolean isequals(Clusters c, double error){
		if (c == null || this.clusters.size() != c.clusters.size())
			return false;
		else{
			for (ClusterId id: clusters.keySet()){
				Cluster c1 = clusters.get(id);
				Cluster c2 = c.clusters.get(id);
				double dist = KmeansUtil.euclideandistance(c1.data, c2.data);
				
				System.out.println(String.format(
				        "----Kmeans----\ncur_dist:%.2f\tthreshold:%.2f\n", dist, error));
				if (c2 == null || dist > error)
				  return false;
			}

		}
		return true;
	}

	public Clusters(){
		clusters = new Hashtable<ClusterId, Cluster>();
	}

	public Clusters(Path path, FileSystem fs){
		clusters = new Hashtable<ClusterId, Cluster>();		
		try{
			if (fs.getFileStatus(path).isDir()){
				FileStatus [] files = fs.listStatus(path);
				for (FileStatus file : files){
					Path p = file.getPath();					
					if (p.toString().startsWith("part")){
					BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(p)));	
					String line;
					while ((line = fis.readLine()) != null){
						String [] ss = line.split("\\s+");
						clusters.put(new ClusterId(ss[0]),new Cluster(ss[1]));
					}
					}
				}
			}
			else{
				BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));	
				String line;
				while ((line = fis.readLine()) != null){
						String [] ss = line.split("\\s+");
						clusters.put(new ClusterId(ss[0]),new Cluster(ss[1]));
				}
			}			
		}
		catch(Exception ex){
			ex.printStackTrace();	
		}
	}

	public Clusters(Path clustersFile){
		clusters = new Hashtable<ClusterId, Cluster>();		
		try{

			BufferedReader fis = new BufferedReader(new FileReader(clustersFile.toString()));	
			String line;
			while ((line = fis.readLine()) != null){
					String [] ss = line.split("\\s+");
					clusters.put(new ClusterId(ss[0]),new Cluster(ss[1]));
			}
						
		}
		catch(Exception ex){
			ex.printStackTrace();	
		}			
	}

	public void addclusters(Path clusterFile){
		if (clusters != null){		
			Clusters c = new Clusters(clusterFile);
			clusters.putAll(c.clusters);
		}
	}

	public void addclusters(Clusters c){
		if (clusters != null)		
			clusters.putAll(c.clusters);		
	}
}
