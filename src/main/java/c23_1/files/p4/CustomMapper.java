package c23_1.files.p4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String name = rowData[4].trim().toLowerCase();
    String country = rowData[7].trim().toLowerCase();
    String accountCreated = rowData[8].trim();
    Text newKey = new Text(country);
    Text newValue = new Text(name + "," + accountCreated);

    outputCollector.collect(newKey, newValue);
  }
}
