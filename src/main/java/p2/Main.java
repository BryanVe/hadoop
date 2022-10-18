package p2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
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
    String country = rowData[7].trim().toLowerCase();
    String card = rowData[3].trim().toLowerCase();
    float price = Float.parseFloat(rowData[2].trim());
    String newValue = country + "-" + card + "-" + price;

    outputCollector.collect(new Text(country), new Text(newValue));
  }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readCountries = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String countryKey = key.toString();

    if (!readCountries.contains(countryKey)) {
      readCountries.add(countryKey);

      List<CountryCardPrice> elements = new ArrayList<>();

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("-");
        String currentCountry = current[0];
        String currentCard = current[1];
        float currentPrice = Float.parseFloat(current[2]);

        elements.add(new CountryCardPrice(currentCountry, currentCard, currentPrice));
      }

      List<CountryCardPrice> elementsFromCurrentCountry = elements
        .stream()
        .filter(e -> {
          String elementCountry = e.getCountry();

          return elementCountry.equals(countryKey);
        })
        .collect(Collectors.toList());
      List<String> cardsFromCurrentCountry = elementsFromCurrentCountry
        .stream()
        .map(CountryCardPrice::getCard)
        .distinct()
        .collect(Collectors.toList());
      List<CountryCardPrice> totalSpentByCard = new ArrayList<>();

      cardsFromCurrentCountry.forEach(card -> {
        AtomicReference<Float> totalFromCurrentCard = new AtomicReference<>((float) 0);

        elementsFromCurrentCountry.forEach(e -> {
          if (e.getCard().equals(card)) totalFromCurrentCard.updateAndGet(v -> v + e.getPrice());
        });
        totalSpentByCard.add(new CountryCardPrice(countryKey, card, totalFromCurrentCard.get()));
      });

      float max = 0;
      String maxCard = "";

      for (CountryCardPrice e : totalSpentByCard)
        if (e.getPrice() > max) {
          max = e.getPrice();
          maxCard = e.getCard();
        }

      // Add the current country to the list of countries to avoid collect it again
      String newValue = maxCard + " " + max;

      outputCollector.collect(new Text(countryKey), new Text(newValue));
    }
  }
}

class CountryCardPrice {
  private final String country;
  private final String card;
  private final float price;

  public CountryCardPrice(String country, String card, float price) {
    this.country = country;
    this.card = card;
    this.price = price;
  }

  public String getCountry() {
    return country;
  }

  public String getCard() {
    return card;
  }

  public float getPrice() {
    return price;
  }
}
