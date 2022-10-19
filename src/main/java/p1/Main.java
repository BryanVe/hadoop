package p1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
        JobClient jobClient = new JobClient();

        // Create a configuration object for the job
        JobConf jobConf = new JobConf(Main.class);

        // Config to run locally
        jobConf.set("fs.defaultFS", "local");
        jobConf.set("mapreduce.job.maps", "1");
        jobConf.set("mapreduce.job.reduces", "1");

        // Set a name of the Job
        jobConf.setJobName("SalePerCountry");

        // Specify data type of output key and value
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(IntWritable.class);

        // Specify names of Mapper and Reducer Class
        jobConf.setMapperClass(CustomMapper.class);
        jobConf.setReducerClass(CustomReducer.class);

        // Specify formats of the data type of input and output
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        // Set input and output directories using command line arguments,
        // arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

        // FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
        // FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));
        // File config to run locally
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        System.out.println(Arrays.toString(files));
        FileInputFormat.setInputPaths(jobConf, new Path(files[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(files[1]));

        jobClient.setConf(jobConf);
        try {
            // run job
            JobClient.runJob(jobConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
