
To Compile the Source Code:-

javac -cp .:$MPJ_HOME/lib/mpj.jar MPIPageRank.java

To Execute the Program: -

mpjrun.sh -np <Num of Processes> MPIPageRank <Input_File> <Output_File> <Iterations> <Delta_Value>
