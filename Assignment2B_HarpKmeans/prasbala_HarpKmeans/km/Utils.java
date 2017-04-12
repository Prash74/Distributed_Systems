package edu.iu.km;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.resource.DoubleArray;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;


/*
 * generate data and initial centroids
 */
public class Utils { 
	private final static int DATA_RANGE = 10;
		
	static void generateData(int numOfDataPoints, int vectorSize, int numMapTasks, FileSystem fs, String localDirStr, Path dataDir) throws IOException, InterruptedException, ExecutionException {
	    int numOfpointFiles = numMapTasks;
		int pointsPerFile = numOfDataPoints / numOfpointFiles;
		int pointsRemainder = numOfDataPoints % numOfpointFiles;
	    System.out.println("Writing " + numOfDataPoints + " vectors to "+ numMapTasks +" file evenly");
	    
	    // Check data directory
	    if (fs.exists(dataDir)) {
	    	fs.delete(dataDir, true);
	    }
	    // Check local directory
	    File localDir = new File(localDirStr);
	    // If existed, regenerate data
	    if (localDir.exists() && localDir.isDirectory()) {
	    	for (File file : localDir.listFiles()) {
	    		file.delete();
	    	}
	    	localDir.delete();
	    }
	    
	    if (localDir.mkdir()) {
	    	System.out.println("Directory: " + localDirStr + " created");
	    }
	    if (pointsPerFile == 0) {
	    	throw new IOException("No point to write.");
	    }
	    
	    double point;
	    int hasRemainder=0;
	    Random random = new Random();
	    for (int k = 0; k < numOfpointFiles; k++) {
	    	try {
			String filename =Integer.toString(k);
	    		File file = new File(localDirStr + File.separator + "data_" + filename);
	    		FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);
			if(pointsRemainder > 0){
		    		hasRemainder = 1;
		    		pointsRemainder--;
	    		}else{
	    			hasRemainder = 0;
	    		}

		    	int pointsForCurrent =  pointsPerFile + hasRemainder;
		    	for (int i = 0; i < pointsForCurrent; i++) {
	    			for (int j = 0; j < vectorSize; j++) {
	    				point = random.nextDouble() * DATA_RANGE;
	    				if(j == vectorSize-1){
	    					bw.write(point + "");
	    					bw.newLine();
	    				}else{
	    					bw.write(point + " ");
	    				}
	    			}
	    		}
	    		bw.close();
	    		System.out.println(pointsForCurrent + "written to file " + filename);
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}
	    }
	    Path localPath = new Path(localDirStr);
	    fs.copyFromLocalFile(localPath, dataDir);
	  }
	
	
	static void generateInitialCentroids(int numCentroids, int vectorSize, Configuration configuration, Path cDir, FileSystem fs, int JobID) throws IOException {
	    Random random = new Random();
	    double[] data = null;
	    if (fs.exists(cDir))
	    	fs.delete(cDir, true);
	    if (!fs.mkdirs(cDir)) {
	    	throw new IOException("Unable to create " + cDir.toString());
	    }
	    
	    data = new double[numCentroids * vectorSize];
	    for (int i = 0; i < data.length; i++) {
	    	data[i] = random.nextDouble() * DATA_RANGE;
	    }

	    Path initClustersFile = new Path(cDir, KMeansConstants.CENTROID_FILE_PREFIX+JobID);
	    System.out.println("Generating centroid data." + initClustersFile.toString());
	    
	    FSDataOutputStream out = fs.create(initClustersFile, true);
	    BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
	    for (int i = 0; i < data.length; i++) {
	    	if ((i % vectorSize) == (vectorSize - 1)) {
	    		bw.write(data[i] + "");
	    		bw.newLine();
	    	} else {
	    		bw.write(data[i] + " ");
	    	}
	    }
	    bw.flush();
	    bw.close();
	    System.out.println("Centroids written");
	  }
	 
	//calculate Euclidean distance.
    	static double calcEucDistSquare(DoubleArray aPoint, DoubleArray otherPoint, int vectorSize){
        	double dist=0;
        	for(int i=0; i < vectorSize; i++){
            		dist += Math.pow(aPoint.get()[i]-otherPoint.get()[i],2);
        	}
        return Math.sqrt(dist);
    }

    static void loadCentroids(Table<DoubleArray> cenTable, int vectorSize, String cFileName, Configuration configuration) throws IOException {
        Path cPath = new Path(cFileName);
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream in = fs.open(cPath);
        BufferedReader br = new BufferedReader( new InputStreamReader(in));
        String line = "";
        String[] vector=null;
        int partitionId=0;
        while((line = br.readLine()) != null){
            vector = line.split("\\s+");
            if(vector.length != vectorSize){
                System.out.println("Errors while loading centroids.");
                System.exit(-1);
            }else{
                double[] aCen = new double[vectorSize+1];

                for(int i=0; i<vectorSize; i++){
                    aCen[i] = Double.parseDouble(vector[i]);
                }
                aCen[vectorSize]=0;
                Partition<DoubleArray> ap = new Partition<DoubleArray>(partitionId, new DoubleArray(aCen, 0, vectorSize + 1));
                cenTable.addPartition(ap);
                partitionId++;
            }
        }
    }

    //load data form HDFS
    static ArrayList<DoubleArray> loadData(List<String> fileNames, int vectorSize, Configuration conf) throws IOException{
        ArrayList<DoubleArray> data = new ArrayList<DoubleArray> ();
        for(String filename: fileNames){
            FileSystem fs = FileSystem.get(conf);
            Path dPath = new Path(filename);
            FSDataInputStream in = fs.open(dPath);
            BufferedReader br = new BufferedReader( new InputStreamReader(in));
            String line="";
            String[] vector=null;
            while((line = br.readLine()) != null){
                vector = line.split("\\s+");

                if(vector.length != vectorSize){
                    System.out.println("Errors while loading data.");
                    System.exit(-1);
                }else{
                    double[] aDataPoint = new double[vectorSize];

                    for(int i=0; i<vectorSize; i++){
                        aDataPoint[i] = Double.parseDouble(vector[i]);
                    }
                    DoubleArray da = new DoubleArray(aDataPoint, 0, vectorSize);
                    data.add(da);
                }
            }
        }
        return data;
    }
}
