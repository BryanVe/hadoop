package p8;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] singleRowData = valueString.split(",");

    String country = singleRowData[7].trim();
    String name = singleRowData[4].trim();
    String accountDate = singleRowData[8].trim();

    if (country.isEmpty() || name.isEmpty() || accountDate.isEmpty()) return;

    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm");

    try {
      dateFormat.parse(accountDate);
    } catch (ParseException e) {
      return;
    }

    outputCollector.collect(new Text(country + ","), new Text(name + "," + accountDate));
  }
}


class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    String recently = "";
    Date recentlyDate = new Date(Long.MIN_VALUE);

    while (values.hasNext()) {
      String[] singleRowData = values.next().toString().split(",");

      SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm");

      Date date;

      try {
        date = dateFormat.parse(singleRowData[1]);
      } catch (ParseException e) {
        return;
      }

      if (date.compareTo(recentlyDate) > 0) {
        recently = singleRowData[0];
        recentlyDate = date;
      }
    }

    output.collect(key, new Text(recently));
  }
}
