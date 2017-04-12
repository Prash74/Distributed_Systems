
import java.io.*;
import java.util.*;

public class SequentialPageRank {
    // adjacency matrix read from file
    private HashMap<Integer, ArrayList<Integer>> adjMatrix = new HashMap<Integer, ArrayList<Integer>>();
    // input file name
    private String inputFile = "";
    // output file name
    private String outputFile = "";
    // number of iterations
    private int iterations = 10;
    // damping factor
    private double df = 0.85;
    // number of URLs
    private int totalURLs = 0;
    // calculating rank values
    private HashMap<Integer, Double> rankValues = new HashMap<Integer, Double>();
    // delta value to stop the loop
    private Double delta = .001;

    public void parseArgs(String[] args) {

        this.inputFile = args[0];
        this.outputFile = args[1];
        this.iterations = Integer.parseInt(args[2]);
        this.df = Double.parseDouble(args[3]);

    }

    public void loadInput() throws IOException {

        FileReader read = new FileReader(new File(this.inputFile));
        BufferedReader buffRead = new BufferedReader(read);
        String ln;
        this.adjMatrix = new HashMap<>();
        while((ln = buffRead.readLine())!=null){
            Scanner nodes = new Scanner(ln);
            ArrayList<Integer> ref = new ArrayList<>();
            // pull out the first node to form the hashMap key
            int node = nodes.nextInt();
            // pull out the subsequent nodes to form the hashMap values
            while(nodes.hasNextInt()){
                ref.add(nodes.nextInt());
            }
            adjMatrix.put(node,ref);
            nodes.close();
        }
        read.close();
    }

    public void calculatePageRank() {

        float numOfUrls;
        numOfUrls = Collections.max(this.adjMatrix.keySet());
        Double initializePageRank = (double) (1 / (numOfUrls+1));
        for (Integer key : adjMatrix.keySet()){
            this.rankValues.put(key,initializePageRank);
            if(adjMatrix.get(key).size()==0){
                ArrayList<Integer> list = new ArrayList<>();
                for(int i=0;i<numOfUrls;i++){
                	if (i==key){
                        continue;
                    }
                    else{
                        list.add(i);
                    }
                }
                adjMatrix.put(key,list);
            }
        }

        for(int itrs = 1; itrs <= this.iterations; itrs++){
            for (int page=0; page<=numOfUrls; page++){
                Double dampFact = ((1-this.df)/numOfUrls);
                Double intPageRank = 0.0;
                for(Integer key : adjMatrix.keySet()){
                    if(adjMatrix.get(key).contains(page)) {
                        intPageRank += (this.rankValues.get(key) / this.adjMatrix.get(key).size());
                    }
                }
                intPageRank = dampFact + (this.df * intPageRank);
                this.rankValues.put(page,intPageRank);
            }
        }
        try{
        PrintWriter writer = new PrintWriter(this.outputFile);
        for (int page=0; page<=numOfUrls; page++){
        	writer.println(page+ "\t" + this.rankValues.get(page));
        }
        writer.close();
        }
        catch (IOException ex) {
           System.out.println("File Error");
        }
    }

    public void printValues() throws IOException {
        List <Double> listPR = new ArrayList<Double>(this.rankValues.values());
        Collections.sort(listPR);
        Collections.reverse(listPR);
        listPR = listPR.subList(0, 10);
        System.out.println("Number of Iterations : "+this.iterations);
        for(Double key: listPR){
        	for (Map.Entry<Integer, Double> e : rankValues.entrySet()) {
        	    if(e.getValue()==key){
        	        System.out.println("Page:" + e.getKey() + "\t\t" + "PageRank:"+key);
        	    }
        	}
        }

    }

    public static void main(String[] args) throws IOException {
        SequentialPageRank sequentialPR = new SequentialPageRank();

        sequentialPR.parseArgs(args);
        sequentialPR.loadInput();
        sequentialPR.calculatePageRank();
        sequentialPR.printValues();
    }
}
