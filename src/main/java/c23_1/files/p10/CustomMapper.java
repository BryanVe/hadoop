package c23_1.files.p10;

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
    String product = rowData[1].trim().toLowerCase();
    String price = rowData[2].trim();
    String country = rowData[7].trim().toLowerCase();
    String latitude = rowData[10].trim();
    String longitude = rowData[11].trim();
    Text newKey = new Text(country);
    Text newValue = new Text(product + "/" + price + "/" + latitude + "/" + longitude);

    outputCollector.collect(newKey, newValue);
  }
}
