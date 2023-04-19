package c23_1.files.p10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    if (key.get() == 0) {
      return;
    }
    String valueString = value.toString();
    String[] singleRowData = valueString.split(",");
    output.collect(new Text(singleRowData[7].trim() + ',' + singleRowData[5].trim()),
      new Text(singleRowData[10] + ',' + singleRowData[11]));
  }
}
