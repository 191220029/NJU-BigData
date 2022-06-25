import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import java.util.List;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.Vector;

public class CourseUniversityJoiner {
    private static class CourseUniversity implements WritableComparable<CourseUniversity> {
        private String c_key = "";
        private String c_name = "";
        private String c_subject = "";
        private String c_hours = "";
        private String u_key = "";
        private String u_name = "";
        private String u_webpage = "";
        private int num = 0;

        public CourseUniversity(){
            c_key = "";
            c_name = "";
            c_subject = "";
            c_hours = "";
            u_key = "";
            u_name = "";
            u_webpage = "";
            num = 0;
        }

        public CourseUniversity(CourseUniversity uc){
            c_key = uc.c_key;
            c_name = uc.c_name;
            c_subject = uc.c_subject;
            c_hours = uc.c_hours;
            u_key = uc.u_key;
            u_name = uc.u_name;
            u_webpage = uc.u_webpage;
            num = uc.num;
        }

        public void setnum(int number){
            num = number;
        }

        public void setCourse(String course) {
            String s[] = course.split("\\|");
            c_key = s[0];
            c_name = s[1];
            c_subject = s[2];
            c_hours = s[3];
            u_key = "";
            u_name = "";
            u_webpage = "";
        }

        public void setUniversity(String university) {
            String s[] = university.split("\\|");
            c_key = "";
            c_name = "";
            c_subject = "";
            c_hours = "";
            u_key =s[0];
            u_name = s[1];
            u_webpage = s[4];
        }

        public String getckey(){
            return c_key;
        }

        @Override
        public int compareTo(CourseUniversity o) {
            if(c_key.equals(c_key))
                return u_key.compareTo(o.u_key);
            return c_key.compareTo(o.c_key);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(c_key);
            dataOutput.writeUTF(c_name);
            dataOutput.writeUTF(c_subject);
            dataOutput.writeUTF(c_hours);
            dataOutput.writeUTF(u_key);
            dataOutput.writeUTF(u_name);
            dataOutput.writeUTF(u_webpage);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            c_key = dataInput.readUTF();
            c_name = dataInput.readUTF();
            c_subject = dataInput.readUTF();
            c_hours = dataInput.readUTF();
            u_key = dataInput.readUTF();
            u_name = dataInput.readUTF();
            u_webpage = dataInput.readUTF();
        }

        @Override
        public String toString(){
            if (!c_key.equals(""))
                return c_key + "|" + c_name + "|" + c_subject + "|" +c_hours;
            else return u_key + "|" + u_name +"|" + u_webpage;
        }


    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "course university");
            job.setJarByClass(CourseUniversityJoiner.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(CourseUniversityJoinMapper.class);
            job.setReducerClass(CourseUniversityJoinReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CourseUniversity.class);
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

    private static class CourseUniversityJoinMapper extends Mapper<Object, Text, Text, CourseUniversity> {
        private Text k = new Text();
        private CourseUniversity v = new CourseUniversity();
        private Boolean courseFlag = true;
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Random rd=new Random();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            if(fileName.equals("university.tbl"))
                courseFlag = false;
            else if(fileName.equals("course.tbl"))
                courseFlag = true;
            else
                return ;
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                int randint=rd.nextInt(100);
                if(courseFlag){
                    k.set(String.valueOf(randint));
                    v.setCourse(line);
                    v.setnum(randint);
                    context.write(k, v);
                }
                else{
                    v.setUniversity(line);
                    for (int i=0;i<100;i++){
                        k.set(String.valueOf(i));
                        v.setnum(i);
                        context.write(k, v);
                    }
                }
            }
        }
    }

    private static class CourseUniversityJoinReducer extends Reducer<Text, CourseUniversity, Text, NullWritable> {
        Text k = new Text();
        Text a = new Text();
        NullWritable v = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<CourseUniversity> values, Context context) throws IOException, InterruptedException {
            Vector<CourseUniversity> c = new Vector<CourseUniversity>();
            Vector<CourseUniversity> u = new Vector<CourseUniversity>();
            for (CourseUniversity value:values) {
                CourseUniversity cu=new CourseUniversity(value);
                if(!cu.getckey().equals("")) {
                    c.add(cu);
                }
                else {
                    u.add(cu);
                }

            }
            for (CourseUniversity course: c){
                for (CourseUniversity university: u){
                    if (course.num==university.num) {
                        k.set(course.toString() + "|" + university.toString());
                        context.write(k, v);
                    }
                }
            }
        }
    }
}