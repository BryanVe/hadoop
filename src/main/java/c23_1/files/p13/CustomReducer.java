package c23_1.files.p13;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    float total = 0;
    List<String> names = new ArrayList<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split("/");
      String name = data[0];
      float price = Float.parseFloat(data[1]);
      total += price;
      names.add(name);
    }

    String namesAsString = names.toString();
    outputCollector.collect(key, new Text(total + " " + namesAsString));
  }
}
