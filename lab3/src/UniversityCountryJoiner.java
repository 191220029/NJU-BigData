import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Vector;

public class UniversityCountryJoiner {
    private static class UniversityCountry implements WritableComparable<UniversityCountry> {
        private String u_key = "";
        private String u_name = "";
        private String u_webpage = "";
        private String n_name = "";
        private String key = "";

        public UniversityCountry(){
            u_key = "";
            u_name = "";
            u_webpage = "";
            n_name = "";
        }

        public UniversityCountry(UniversityCountry uc){
            u_key = uc.u_key;
            u_name = uc.u_name;
            u_webpage = uc.u_webpage;
            n_name = uc.n_name;
        }

        public String setCountry(String country) {
            String s[] = country.split("\\|");
            u_key = "";
            u_name = "";
            u_webpage = "";
            n_name = s[1];
            return s[0];
        }

        public String setUniversity(String university) {
            String s[] = university.split("\\|");
            u_key = s[0];
            u_name = s[1];
            u_webpage = s[4];
            n_name = "";
            return s[2];
        }

        public void setN_name(String name) {
            n_name = name;
        }

        public String getN_name() {
            return n_name;
        }

        @Override
        public int compareTo(UniversityCountry o) {
            return u_key.compareTo(o.u_key);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(u_key);
            dataOutput.writeUTF(u_name);
            dataOutput.writeUTF(u_webpage);
            dataOutput.writeUTF(n_name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            u_key = dataInput.readUTF();
            u_name = dataInput.readUTF();
            u_webpage = dataInput.readUTF();
            n_name = dataInput.readUTF();
        }

        @Override
        public String toString() {
            return u_key + '|' + u_name + '|' + u_webpage + '|' + n_name;
        }

    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "university country");
            job.setJarByClass(UniversityCountryJoiner.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(UniversityCountryJoinMapper.class);
            job.setReducerClass(UniversityCountryJoinReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(UniversityCountry.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UniversityCountryJoinMapper extends Mapper<Object, Text, Text, UniversityCountry> {
        private Text k = new Text();
        private UniversityCountry v = new UniversityCountry();
        private Boolean countryFlag = true;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.equals("university.tbl"))
                countryFlag = false;
            else if(fileName.equals("country.tbl"))
                countryFlag = true;
            else
                return ;
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                if(countryFlag)
                    k.set(v.setCountry(line));
                else
                    k.set(v.setUniversity(line));
                context.write(k, v);
            }
        }
    }

    private static class UniversityCountryJoinReducer extends Reducer<Text, UniversityCountry, Text, NullWritable> {
        String n_name;
        Text k = new Text();
        NullWritable v = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<UniversityCountry> values, Context context) throws java.io.IOException, java.lang.InterruptedException {
            Vector<UniversityCountry> UCList = new Vector<UniversityCountry>();
            for (UniversityCountry value:values){
                UniversityCountry uc = new UniversityCountry(value);
                UCList.add(uc);
            }
            for(UniversityCountry uc : UCList) {
                if(!uc.getN_name().equals("")) {
                    n_name = uc.getN_name();
                    break;
                }
            }
            for(UniversityCountry uc : UCList) {
                if(uc.getN_name().equals("")) {
                    uc.setN_name(n_name);
                    k.set(uc.toString());
                    context.write(k, v);
                }
            }
        }
    }
}