package c23_1.files.p13;

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

    if (!name.startsWith("s")) return;

    String paymentType = rowData[3].trim().toLowerCase();
    String price = rowData[2].trim();
    Text newKey = new Text(paymentType);
    Text newValue = new Text(name + "/" + price);

    outputCollector.collect(newKey, newValue);
  }
}
