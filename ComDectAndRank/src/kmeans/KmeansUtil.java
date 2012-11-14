package kmeans;

public class KmeansUtil{
	public static double euclideandistance(double [] a, double [] b){
		double dis = 0;
		if (a.length == b.length){
			for (int i = 0; i < a.length; i++){
				double d = a[i] - b[i];				
				dis = dis + d * d; 
			}
			return Math.sqrt(dis); 		
		}
		else{
			return -1;
		}
	}
}
