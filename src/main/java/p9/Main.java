package p9;

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

// Encontrar el nombre y ciudad de las personas que compraron el producto más caro por país
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

    // Set input and output directories using command line arguments,
    // arg[0] = name of input directory on HDFS, and arg[1] =  name of output directory to be created to store the output file.

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

class CountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String name = rowData[4].trim().toLowerCase();
    String country = rowData[7].trim().toLowerCase();
    String city = rowData[5].trim().toLowerCase();
    String price = rowData[2].trim().toLowerCase();

    String newKeyAndValue = name + "/" + country + "/" + city + "/" + price;

    outputCollector.collect(new Text(newKeyAndValue), new Text(newKeyAndValue));
  }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readCountry = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] compoundKey = key.toString().split("/");
    String countryKey = compoundKey[1];

    if (!readCountry.contains(countryKey)) {
      readCountry.add(countryKey);

      String name = compoundKey[0];
      String city = compoundKey[2];
      float price = Float.parseFloat(compoundKey[3]);
      System.out.println("{ name: " + name + ", city: " + city + ", price: " + price + " }");

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("/");
        String currentName = current[0];
        String currentCountry = current[1];
        String currentCity = compoundKey[2];
        float currentPrice = Float.parseFloat(compoundKey[3]);

        if (countryKey.equals(currentCountry) && currentPrice > price) {
          name = currentName;
          city = currentCity;
          price = currentPrice;
        }
      }

      System.out.println(name + " " + city);

      outputCollector.collect(new Text(name), new Text(city));
    }
  }
}
