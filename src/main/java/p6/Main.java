package p6;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
    jobConf.setMapperClass(CountryMapper.class);
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

class CountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");

    String[] accountCreatedDate = rowData[8].trim().split(" ");
    String[] dateArray = accountCreatedDate[0].split("/");
    String year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
    String month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
    String day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
    String[] timeArray = accountCreatedDate[1].split(":");
    String hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
    String minutes = timeArray[1];
    String finalDateAccountCreated = year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";

    String[] lastLoginDate = rowData[9].trim().split(" ");
    dateArray = lastLoginDate[0].split("/");
    year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
    month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
    day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
    timeArray = lastLoginDate[1].split(":");
    hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
    minutes = timeArray[1];
    String finalDateLogin = year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";

    int antiquity = Math.abs(
      (int) ChronoUnit.DAYS.between(
        LocalDateTime.parse(finalDateLogin),
        LocalDateTime.parse(finalDateAccountCreated)
      )
    );
    String country = rowData[7].trim().toLowerCase();
    String name = rowData[4].trim().toLowerCase();

    String newKeyAndValue = name + "/" + country + "/" + antiquity;

    outputCollector.collect(new Text(newKeyAndValue), new Text(newKeyAndValue));
  }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readCountry = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] compoundKey = key.toString().split("/");
    String countryKey = compoundKey[1];
    int antiquityKey = Integer.parseInt(compoundKey[2]);

    if (!readCountry.contains(countryKey)) {
      readCountry.add(countryKey);

      String name = compoundKey[0];
      int antiquity = antiquityKey;

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("/");
        String currentName = current[0];
        String currentCountry = current[1];
        int currentAntiquity = Integer.parseInt(current[2]);

        if (countryKey.equals(currentCountry) && currentAntiquity > antiquityKey) {
          name = currentName;
          antiquity = currentAntiquity;
        }
      }

      outputCollector.collect(new Text(name), new Text(antiquity + ""));
    }
  }
}
