
import java.io.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import mpi.MPI;

public class MPIPageRank {

    private HashMap<Integer, List<Integer>> adjMatrix = new HashMap<Integer, List<Integer>>();
    private String inputFile = "";
    private String outputFile = "";
    private int iterations = 10;
    private double df = 0.85;
    private int totalURLs = 0;
    private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();
    private Double delta = .001;

    public void parseArgs(String[] args) {
        this.inputFile = args[3];
        this.outputFile = args[4];
        this.iterations = Integer.parseInt(args[5]);
	      this.delta = Double.parseDouble(args[6]);
    }

    public void loadInput() throws IOException {
        try{
            FileReader reader = new FileReader(new File(this.inputFile));
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            this.adjMatrix = new HashMap<>();
            while ((line = bufferedReader.readLine()) != null) {
                Scanner nodes = new Scanner(line);
                ArrayList<Integer> referred = new ArrayList<>();
                int node = nodes.nextInt();
                while (nodes.hasNextInt()) {
                    referred.add(nodes.nextInt());
                }
                adjMatrix.put(node, referred);
                nodes.close();
            }
            reader.close();
            this.totalURLs = Collections.max(this.adjMatrix.keySet()) + 1;
          }
        catch (FileNotFoundException e) {
            System.out.println("Input file not found, " + e.getMessage());
        }
    }

    public void calculatePageRank(int rank, HashMap<Integer, List<Integer>> chunkPRTab) {

        HashMap<Integer, Double> interPRtab = new HashMap<Integer, Double>();
        Double initializePR = (double) (1 / this.totalURLs);

        for(int x = 0; x < this.totalURLs; x++){
            rankValues.put(x, 1.0/this.totalURLs);
        }

        //for (Integer i=0 ; i<iterations ; i++){
        boolean chk = true;
        int iter = this.iterations;
        do{
            //Initialize intermediate rank values
            for (Integer key : rankValues.keySet()){
                interPRtab.put(key, 0.0);
            }
            for (Integer webUrl : chunkPRTab.keySet()) {
                if (chunkPRTab.get(webUrl).size() == 0) {
                    ArrayList<Integer> list = new ArrayList<>();
                    for (int i = 0; i < this.totalURLs; i++) {
                        if (i == webUrl) {
                            continue;
                        } else {
                            list.add(i);
                        }
                    }
                    chunkPRTab.put(webUrl, list);
                }
                    for (Integer outNodes : chunkPRTab.get(webUrl)) {
                        Double intPageRank = interPRtab.get(outNodes) + this.df*(rankValues.get(webUrl)/ (chunkPRTab.get(webUrl).size()));
                        interPRtab.put(outNodes, intPageRank);
                    }
            }
            double sendArr[] = new double[this.totalURLs];
            for(int y = 0; y < this.totalURLs; y++){
                sendArr[y] = interPRtab.get(y);
            }
            MPI.COMM_WORLD.Allreduce(sendArr,0, sendArr, 0, sendArr.length, MPI.DOUBLE, MPI.SUM);
            for (Integer key : rankValues.keySet()){
                sendArr[key] += (1.0-df) / this.totalURLs;
            }
            chk = false;
            for(Integer key: interPRtab.keySet()){
                if (Math.abs(sendArr[key] - rankValues.get(key)) > this.delta || iter==0){
                    chk = true;
                }
            }
            for(Integer key : interPRtab.keySet()){
                rankValues.put(key, sendArr[key]);
            }
            iter-=1;
        }while(chk);

    }

    public void printValues() {
        List<Double> listPageRank = new ArrayList<Double>(this.rankValues.values());
        Collections.sort(listPageRank);
        Collections.reverse(listPageRank);
        listPageRank = listPageRank.subList(0, 10);
        for (Double key : listPageRank) {
            for (Map.Entry<Integer, Double> e : rankValues.entrySet()) {
                if (e.getValue() == key) {
                    System.out.println("Link: " + e.getKey() + "\t\t" + "PR Value: " + key);
                }
            }
        }
        try {
            PrintWriter writer = new PrintWriter(this.outputFile);
            for (int page = 0; page <= this.totalURLs; page++) {
                writer.println(page + "\t" + this.rankValues.get(page));
            }
            writer.close();
        } catch (IOException ex) {
            System.out.println("File can't be written, " + ex.getMessage());
        }

    }

    public void mpiRun(String[] cmdLineArgs) throws IOException{
        MPI.Init(cmdLineArgs);
        int rank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        int chunksizeURLs[] = new int[1];

        if (rank == 0) {
            long startTime = System.currentTimeMillis();
            this.loadInput();
            long endTime = System.currentTimeMillis();
            long totalTime = endTime - startTime;
            System.out.println("I/O Time: "+totalTime/1000.0);
            chunksizeURLs[0] = this.totalURLs;
        }
        MPI.COMM_WORLD.Bcast(chunksizeURLs, 0, 1, MPI.INT, 0);

        this.totalURLs = chunksizeURLs[0];


        HashMap<Integer, List<Integer>> chunkPRCalc = new HashMap<Integer, List<Integer>>();
        for(int i = 0; i < this.totalURLs; i++){

            if (rank == 0){
                int localkey;
                int local_urlid = i;
                int localValues[] = new int[this.adjMatrix.get(i).size()];

                if(i%size == 0){
                    localkey = local_urlid;
                    for(int i1 = 0; i1 < this.adjMatrix.get(localkey).size(); i1++){
                        localValues[i1] = this.adjMatrix.get(localkey).get(i1);
                    }

                    List<Integer> tempList = new ArrayList<Integer>();
                    for(Integer val: localValues){
                        tempList.add(val);
                    }
                    chunkPRCalc.put(localkey, tempList);
                }
                else{

                    int[] val_arr = new int [this.adjMatrix.get(local_urlid).size() + 1];
                    val_arr[0] = local_urlid;
                    for(int x = 1; x < this.adjMatrix.get(local_urlid).size() + 1; x++){
                        val_arr[x] = this.adjMatrix.get(local_urlid).get(x-1);
                    }
                    int[] sizeOfVal = new int[1];
                    sizeOfVal[0] = this.adjMatrix.get(local_urlid).size() + 1;

                    MPI.COMM_WORLD.Send(sizeOfVal, 0, 1, MPI.INT, i%size, 2);
                    MPI.COMM_WORLD.Send(val_arr, 0, val_arr.length, MPI.INT, i%size, 3);

                }
            }
            else{

                if(i%size ==rank){
                    int localKey;
                    int[] arraySizeBuf = new int[1];
                    MPI.COMM_WORLD.Recv(arraySizeBuf, 0, 1, MPI.INT, 0, 2);
                    int[] recievedAdjArray = new int[arraySizeBuf[0]];
                    MPI.COMM_WORLD.Recv(recievedAdjArray, 0, recievedAdjArray.length, MPI.INT, 0 , 3);
                    localKey = recievedAdjArray[0];
                    int[] localValues = Arrays.copyOfRange(recievedAdjArray, 1, recievedAdjArray.length);
                    List<Integer> tempList = new ArrayList<Integer>();
                    for(Integer val: localValues){
                        tempList.add(val);
                    }
                    chunkPRCalc.put(localKey, tempList);
                }
            }
        }

        if(rank == 0){
        long stTime = System.currentTimeMillis();
        this.calculatePageRank(rank, chunkPRCalc);
        long eTime = System.currentTimeMillis();
        long totTime = eTime - stTime;
        System.out.println("Job Execution Time: "+totTime/1000.0);
        }
        else{
          this.calculatePageRank(rank, chunkPRCalc);
        }
        if(rank == 0){
            this.printValues();
        }
        MPI.Finalize();
    }

    public static void main(String[] args) throws IOException {

        MPIPageRank pR = new MPIPageRank();
        pR.parseArgs(args);
        pR.mpiRun(args);

    }

}
