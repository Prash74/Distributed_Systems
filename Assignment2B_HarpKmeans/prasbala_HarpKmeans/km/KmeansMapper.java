package edu.iu.km;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.CollectiveMapper;
import org.apache.hadoop.io.Text;

import edu.iu.harp.partition.Partition;
import edu.iu.harp.partition.Table;
import edu.iu.harp.example.DoubleArrPlus;
import edu.iu.harp.resource.DoubleArray;

public class KmeansMapper  extends CollectiveMapper<String, String, Object, Object> {
	private int vectorSize;
	private int iteration;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration configuration = context.getConfiguration();
		/*
		 * initialization
		 */
		long startTime = System.currentTimeMillis();
		vectorSize =configuration.getInt(KMeansConstants.VECTOR_SIZE, 20);
		iteration = configuration.getInt(KMeansConstants.NUM_ITERATONS, 1);
		long endTime = System.currentTimeMillis();
	}

	protected void mapCollective( KeyValReader reader, Context context) throws IOException, InterruptedException {
		/*
		 * vals in the keyval pairs from reader are data file paths.
		 * read data from file paths.
		 * load initial centroids
		 * do{
		 * 	computations
		 *  generate new centroids
		 * }while(<maxIteration)
		 * 
		 */
		LOG.info("Start collective mapper.");
		long startTime = System.currentTimeMillis();
		List<String> pointFiles = new ArrayList<String>();
		while (reader.nextKeyValue()) {
			String key = reader.getCurrentKey();
			String value = reader.getCurrentValue();
			LOG.info("Key: " + key + ", Value: " + value);
			pointFiles.add(value);
		}
		Configuration conf = context.getConfiguration();
		runKmeans(pointFiles, conf, context);
		LOG.info("Total iterations in master view: " + (System.currentTimeMillis() - startTime));
	}
	
	private void broadcastCentroids( Table<DoubleArray> cenTable) throws IOException{
		//broadcast centroids
		boolean isSuccess = false;
		try {
			isSuccess = broadcast("main", "broadcast-centroids", cenTable, this.getMasterID(), false);
		} catch (Exception e) {
			LOG.error("Fail to bcast.", e);
		}
		if (!isSuccess) {
			throw new IOException("Fail to bcast");
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void findNearestCenter(Table<DoubleArray> cenTable, Table<DoubleArray> previousCenTable, ArrayList<DoubleArray> dataPoints){
		double err=0;
		for(DoubleArray aPoint: dataPoints){
			//for each data point, find the nearest centroid
			double minDist = Double.MAX_VALUE;
			double tempDist = 0;
			int nearestPartitionID = -1;
			for(Partition ap: previousCenTable.getPartitions()){
				DoubleArray aCentroid = (DoubleArray) ap.get();

				// calculate euclidean distance
				tempDist = Utils.calcEucDistSquare(aPoint,aCentroid,vectorSize);

				if (tempDist < minDist) {
					minDist = tempDist;
					nearestPartitionID = ap.id(); //nearest centroid
				}
			}
			err+=minDist;
			double[] partial = new double[vectorSize+1];
			for(int j=0; j < vectorSize; j++)
				partial[j] = aPoint.get()[j];
			
			partial[vectorSize]=1;

			if(cenTable.getPartition(nearestPartitionID) == null){
				Partition<DoubleArray> tmpAp = new Partition<DoubleArray>(nearestPartitionID, new DoubleArray(partial, 0, vectorSize+1));
				cenTable.addPartition(tmpAp);

			}else{
				Partition<DoubleArray> apInCenTable = cenTable.getPartition(nearestPartitionID);
				for(int i=0; i < vectorSize +1; i++){
					apInCenTable.get().get()[i] += partial[i];
				}
			}
		}
		System.out.println("Errors: "+err);
	}
	
	private void runKmeans(List<String> fileNames, Configuration conf, Context context) throws IOException {
		Table<DoubleArray> cenTable = new Table<>(0, new DoubleArrPlus());// = new Table<>(new DoubleArrPlus());

		if (this.isMaster()) {
			Utils.loadCentroids(cenTable, vectorSize, conf.get(KMeansConstants.CFILE), conf);
		}

		//broadcast centroids
		broadcastCentroids(cenTable);

		ArrayList<DoubleArray> dataPoints = Utils.loadData(fileNames, vectorSize, conf);
		Table<DoubleArray> previousCenTable =  null;

		for(int iter=0; iter < iteration; iter++){
			previousCenTable =  cenTable;
			cenTable = new Table<>(0, new DoubleArrPlus());

			System.out.println("\n\nIteration:"+iter);

			//nearest centroid recompute
			findNearestCenter(cenTable, previousCenTable, dataPoints);

			allreduce("main", "allreduce"+iter, cenTable);

			//udpate new centroids
			updateCenters(cenTable);

		}
		if(this.isMaster()){
			outputCentroids(cenTable, conf, context);
		}
	}
	
	@SuppressWarnings("unused")
	private void updateCenters(Table<DoubleArray> cenTable){
		for( Partition<DoubleArray> partialCenTable: cenTable.getPartitions()){
			double[] doubles = partialCenTable.get().get();
			double N = doubles.length - 1; 
			for (int i=0; i<N; ++i) {
				System.out.print(doubles[i]+", ");
				doubles[i] /= doubles[vectorSize];
			}
			doubles[vectorSize] = 0;
			System.out.println("");
		}
		System.out.println("\n\n\nCalculated new centroids");
	}

	private void outputCentroids(Table<DoubleArray>  cenTable,Configuration conf, Context context){
		String output="";
		for( Partition<DoubleArray> ap: cenTable.getPartitions()){
			double res[] = ap.get().get();
			for(int i=0; i<vectorSize;i++)
				output+= res[i]+"\t";
			output+="\n";
		}
		try {
			context.write(null, new Text(output));
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
	}
}