package p3;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
    jobConf.setMapperClass(p3.CountryMapper.class);
    jobConf.setReducerClass(p3.CustomReducer.class);

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
    String country = rowData[7].trim().toLowerCase();
    String name = rowData[4].trim().toLowerCase();
    float price = Float.parseFloat(rowData[2].trim());
    String newValue = name + "/" + country + "/" + price;

    outputCollector.collect(new Text(name), new Text(newValue));
  }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readPeople = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String nameKey = key.toString();

    if (!readPeople.contains(nameKey)) {
      readPeople.add(nameKey);

      List<NameCountryPrice> elements = new ArrayList<>();

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("/");
        String currentName = current[0];
        String currentCountry = current[1];
        float currentPrice = Float.parseFloat(current[2]);

        elements.add(new NameCountryPrice(currentName, currentCountry, currentPrice));
      }

      List<NameCountryPrice> elementsFromCurrentPerson = elements
        .stream()
        .filter(e -> {
          String elementName = e.getName();

          return elementName.equals(nameKey);
        })
        .collect(Collectors.toList());
      List<String> countriesFromCurrentPerson = elementsFromCurrentPerson
        .stream()
        .map(NameCountryPrice::getCountry)
        .distinct()
        .collect(Collectors.toList());
      List<NameCountryPrice> totalSpentByPerson = new ArrayList<>();

      countriesFromCurrentPerson.forEach(country -> {
        AtomicReference<Float> totalFromCurrentCard = new AtomicReference<>((float) 0);

        elementsFromCurrentPerson.forEach(e -> {
          if (e.getCountry().equals(country)) totalFromCurrentCard.updateAndGet(v -> v + e.getPrice());
        });
        totalSpentByPerson.add(new NameCountryPrice(nameKey, country, totalFromCurrentCard.get()));
      });
      //System.out.println(totalSpentByPerson);

      float max = 0;
      String maxCountry = "";

      for (NameCountryPrice e : totalSpentByPerson)
        if (e.getPrice() > max) {
          max = e.getPrice();
          maxCountry = e.getCountry();
        }

      // Add the current country to the list of countries to avoid collect it again
      String newValue = maxCountry + " " + max;

      outputCollector.collect(new Text(nameKey), new Text(newValue));
    }
  }
}

class NameCountryPrice {
  private final String name;
  private final String country;
  private final float price;

  public NameCountryPrice(String name, String country, float price) {
    this.name = name;
    this.country = country;
    this.price = price;
  }

  public String getName() {
    return name;
  }

  public String getCountry() {
    return country;
  }

  public float getPrice() {
    return price;
  }
}
