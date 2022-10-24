package p7;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class Main {
  public static void main(String[] args) throws IOException {
    JobClient jobClient = new JobClient();

    // Create a configuration object for the job
    JobConf jobConf = new JobConf(p2.Main.class);

    // Config to run locally
    jobConf.set("fs.defaultFS", "local");
    jobConf.set("mapreduce.job.maps", "1");
    jobConf.set("mapreduce.job.reduces", "1");

    // Set a name of the Job
    jobConf.setJobName("SalePerCountryPerCard");

    // Specify data type of output key and value
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);

    // Specify names of Mapper and Reducer Class
    jobConf.setMapperClass(CustomMapper.class);
    jobConf.setReducerClass(CustomReducer.class);

    // Specify formats of the data type of input and output
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setOutputFormat(TextOutputFormat.class);

    if (Dotenv.load().get("HADOOP_ENV").equals("local")) {
      Configuration c = new Configuration();
      String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
      System.out.println(Arrays.toString(files));
      FileInputFormat.setInputPaths(jobConf, new Path(files[0]));
      FileOutputFormat.setOutputPath(jobConf, new Path(files[1]));
    } else {
      System.out.println(Arrays.toString(args));
      FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
      FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));
    }

    jobClient.setConf(jobConf);
    try {
      // run job
      JobClient.runJob(jobConf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    if (key.get() == 0) {
      return;
    }
    String valueString = value.toString();
    String[] singleRowData = valueString.split(",");
    output.collect(new Text(singleRowData[7].trim() + ',' + singleRowData[5].trim()),
            new Text(singleRowData[10] + ',' + singleRowData[11]));
  }
}


class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    double averageLat = 0.0;
    double averageLng = 0.0;
    int i = 0;
    while (values.hasNext()) {
      // replace type of value with the actual type of our value
      String[] vals = values.next().toString().split(",");

      double lat = Double.parseDouble(vals[0]);
      double lng = Double.parseDouble(vals[1]);

      averageLat += lat;
      averageLng += lng;
      i++;
    }
    averageLat = averageLat / i;
    averageLng = averageLng / i;

    output.collect(t_key,
            new Text("avgLat: " + averageLat + ", avgLng: " + averageLng));
  }
}

