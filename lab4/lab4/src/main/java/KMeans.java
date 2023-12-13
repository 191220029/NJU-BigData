import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.LineReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class KMeans {
    private static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private Text k = new Text();
        private Text v = new Text();

        private HashMap<Integer, List<Double>> centers = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException {
            //read "initial_centers", store center vectors in $centers$;
            FileReader fileReader = new FileReader("centers");
            BufferedReader bufferReader = new BufferedReader(fileReader);
            String line;
            while(StringUtils.isNotEmpty(line = bufferReader.readLine())){
                Integer id = Integer.parseInt(line.substring(0, line.indexOf('\t')));
                List<Double> vector = new LinkedList<>();
                for(String item : line.substring(line.indexOf('\t') + 1).split(","))
                    vector.add(Double.parseDouble(item));
//                Integer id = Integer.parseInt(line.substring(0, line.indexOf(':')));
//                String[] vectors = line.substring(line.indexOf(':') + 2).split(",");
//                List<Double> vector = new LinkedList<>();
//                for(String s : vectors)
//                    vector.add(Double.parseDouble(s));
                centers.put(id, vector);
            }
            return;
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            // split s into Id, Vector
            Integer id = Integer.parseInt(input.substring(0, input.indexOf(':')));
            List<Double> vector = new LinkedList<>();
            String[] vectors = input.substring(input.indexOf(':') + 2).split(" ");
            for(String s : vectors)
                vector.add(Double.parseDouble(s));

            // find a nearest center
            double minDist = -1;
            Integer centerId = 0;
            for(Integer i : centers.keySet()){
                List<Double> vectorC = centers.get(i);
                double curDist = 0;
                for(int k = 0; k < vectorC.size(); k++)
                    curDist += Math.pow(Math.abs(vectorC.get(k) - vector.get(k)), 2);
                curDist = Math.sqrt(curDist);
                if(minDist == -1 || minDist > curDist){
                    minDist = curDist;
                    centerId = i;
                }
            }
            StringBuilder builder = new StringBuilder();
            builder.append(id.toString()+"[");
            for(int k = 0; k < vector.size(); k++){
                if(k > 0)
                    builder.append(',');
                builder.append(vector.get(k));
            }
            builder.append(']');
            k.set(centerId.toString());
            v.set(builder.toString());
            context.write(k, v);
        }

    }



    private static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        private Text v = new Text();
        private MultipleOutputs<Text, Text> multiOs;
        @Override
        protected void setup(Context context){
            multiOs = new MultipleOutputs<>(context);
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException, java.lang.InterruptedException {
            StringBuilder builder = new StringBuilder();
            int clusterSize = 0;
            List<Double> averageVector = new LinkedList<>();
            for(Text t : values){
                // $nodeId$'['$vector$']'
                String s = t.toString();
                String[] vectors = s.substring(s.indexOf('[') + 1, s.indexOf(']') - 1).split(",");
                int i = 0;
                // calculate sum of vectors
                for(String item : vectors) {
                    if(clusterSize == 0)
                        averageVector.add(Double.parseDouble(item));
                    else {
                        averageVector.set(i, averageVector.get(i) + Double.parseDouble(item));
                        i++;
                    }
                }
                clusterSize++;
//                builder.append(s + ",");
            }
            // calculate the average vector
            for(int i = 0; i < averageVector.size(); i++) {
//                averageVector.set(i, averageVector.get(i) / clusterSize);
                if(i > 0)
                    builder.append(',');
                builder.append(averageVector.get(i) / clusterSize);

            }
            v.set(builder.toString());
            context.write(key, v);
            multiOs.write(key, v, "out_" + key);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            multiOs.close();
        }
    }


    public static void deletePath(String pathStr, boolean isDeleteDir) throws IOException {
        if(isDeleteDir) pathStr = pathStr.substring(0, pathStr.lastIndexOf('/'));
        Path path = new Path(pathStr);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.delete(path, true);
    }
    public static void copyFile(String from_path, String to_path) throws IOException {
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String str = line.toString() + "\n";
            outputStream.write(str.getBytes());
        }
        lineReader.close();
        outputStream.close();
    }
    public static boolean changedVectors(Integer times, String outDir) throws IOException {
        if (times == 0)
            return true;
        else {
            Configuration conf = new Configuration();
            Path newCenterPath = new Path(outDir + "/part-r-00000");
            Path oldCenterPath = new Path("/lab4/tmp/centers");
            FileSystem fileSystem = newCenterPath.getFileSystem(conf);
            FSDataInputStream newCenterStream = fileSystem.open(newCenterPath);
            FSDataInputStream oldCenterStream = fileSystem.open(oldCenterPath);
            LineReader newLineReader = new LineReader(newCenterStream, conf);
            LineReader oldLineReader = new LineReader(oldCenterStream, conf);
            HashMap<Integer, List<Double>> newCenter = new HashMap<>();
            HashMap<Integer, List<Double>> oldCenter = new HashMap<>();
            Text line = new Text();
            while(newLineReader.readLine(line) > 0){
                String s = line.toString();
                Integer id = Integer.parseInt(s.substring(0, s.indexOf('\t')));
                List<Double> vector = new LinkedList<>();
                for(String item : s.substring(s.indexOf('\t') + 1).split(","))
                    vector.add(Double.parseDouble(item));
                newCenter.put(id, vector);
            }
            while(oldLineReader.readLine(line) > 0){
                String s = line.toString();
                Integer id = Integer.parseInt(s.substring(0, s.indexOf('\t')));
                List<Double> vector = new LinkedList<>();
                for(String item : s.substring(s.indexOf('\t') + 1).split(","))
                    vector.add(Double.parseDouble(item));
                oldCenter.put(id, vector);
            }
            for(Integer id : oldCenter.keySet()){
                List<Double> oldVector = oldCenter.get(id);
                List<Double> newVector = newCenter.get(id);
                if(oldVector.equals(newVector) == false)
                    return true;
            }
            return false;
        }
    }
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Path tmpDir = new Path("/lab4/tmp");
            FileSystem fileSystem = tmpDir.getFileSystem(conf);
            fileSystem.mkdirs(tmpDir);
            //copyFile(args[0] + "/initial_centers", "/lab4/tmp/centers");
            reformatCenters(args[0] + "/initial_centers", "/lab4/tmp/centers");
            int times = 0;
            while(changedVectors(times, args[1]) == true) {
                if(times > 0) {
                    copyFile(args[1]+ "/part-r-00000", "/lab4/tmp/centers");
                    Path outPath = new Path(args[1]);
                    fileSystem.delete(outPath, true);
                    //deletePath(args[1], true);
                }
                Job job = new Job(conf, "KMeans");
                job.addCacheFile(new URI("/lab4/tmp/centers#centers")); //set distributed cache. string after '#' is name.
                job.setJarByClass(KMeans.class);
                job.setInputFormatClass(TextInputFormat.class);
                job.setMapperClass(KMeansMapper.class);
                job.setReducerClass(KMeansReducer.class);
//            job.setPartitionerClass(InvertIndexPartitioner.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(args[0] + "/dataset.data"));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                job.waitForCompletion(true);
                times++;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void reformatCenters(String from_path, String to_path) throws  IOException{
        Path path_from = new Path(from_path);
        Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path_from.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String input = line.toString();
            Integer id = Integer.parseInt(input.substring(0, input.indexOf(':')));
            String vector = input.substring(input.indexOf(':') + 2);
            StringBuilder builder = new StringBuilder();
            builder.append(id);
            builder.append("\t");
            builder.append(vector);
            builder.append("\n");
            outputStream.write(builder.toString().getBytes());
        }
        lineReader.close();
        outputStream.close();
        return;
    }

}