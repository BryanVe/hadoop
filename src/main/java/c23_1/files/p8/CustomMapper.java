package c23_1.files.p8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

        String valueString = value.toString();
        String[] rowData = valueString.split(",");
        String country = rowData[7].trim().toLowerCase();
        String city = rowData[5].trim().toLowerCase();

        String newValue = city + "/" + country;

        outputCollector.collect(new Text(country), new Text(newValue));
    }
}
