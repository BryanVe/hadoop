package p2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) {
        JobClient jobClient = new JobClient();

        // Create a configuration object for the job
        JobConf jobConf = new JobConf(p2.Main.class);

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

        FileInputFormat.setInputPaths(jobConf, new Path(args[1]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[2]));

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

        outputCollector.collect(new Text(country + "-" + card), new Text(country + "-" + card + "-" + price));
    }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
    @Override
    public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, FloatWritable> outputCollector, Reporter reporter) throws IOException {
        float sum = 0;
        String[] compoundKey = key.toString().split("-");
        String countryKey = compoundKey[0];
        String cardKey = compoundKey[1];

        while (iterator.hasNext()) {
            Object value = iterator.next();
            String[] current = value.toString().split("-");
            String currentCountry = current[0];
            String currentCard = current[1];
            float currentPrice = Float.parseFloat(current[2]);

            if (countryKey.equals(currentCountry) && cardKey.equals(currentCard)) {
                sum += currentPrice;
            }
        }

        outputCollector.collect(key, new FloatWritable(sum));
    }
}
