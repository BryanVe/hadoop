package c23_1.files.p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
  IntWritable one = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");
    String state = rowData[6].trim().toLowerCase();

    if (state.length() == 0) return;

    String country = rowData[7].trim().toLowerCase();
    String paymentType = rowData[3].trim().toLowerCase();
    Text newKey = new Text(state + " " + country + " " + paymentType);

    outputCollector.collect(newKey, one);
  }
}
