package p9;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String name = rowData[4].trim().toLowerCase();
    String country = rowData[7].trim().toLowerCase();
    String state = rowData[5].trim().toLowerCase();
    String city = rowData[5].trim().toLowerCase();
    String price = rowData[2].trim().toLowerCase();

    String newKeyAndValue = name + "/" + country + "/" + state + "/" + city + "/" + price;

    outputCollector.collect(new Text(newKeyAndValue), new Text(newKeyAndValue));
  }
}
