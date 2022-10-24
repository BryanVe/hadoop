package p11;

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

// Encontrar el nombre y ciudad de las personas que compraron el producto más caro por país
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
        String[] rowData = valueString.split(",");
        String name = rowData[4].trim().toLowerCase();
        String country = rowData[7].trim().toLowerCase();
        String price = rowData[2].trim();

        outputCollector.collect(new Text(name + "/" + country), new Text(price));
    }
}

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String[] compoundKey = key.toString().split("/");
        String nameKey = compoundKey[0];
        String countryKey = compoundKey[1];

        if (nameKey.startsWith("j") && countryKey.endsWith("s")) {
            int pricesQuantity = 0;
            float sumPrices = 0;
            // Convert iterator to a list
            while (iterator.hasNext()) {
                float currentPrice = Float.parseFloat(iterator.next().toString());
                sumPrices += currentPrice;
                pricesQuantity++;
            }

            outputCollector.collect(new Text(key), new Text(Float.toString(sumPrices / pricesQuantity)));
        }
    }
}
