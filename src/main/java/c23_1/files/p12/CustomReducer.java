package c23_1.files.p12;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // create map for saving frequencies of payment types
    float maxPrice = 0;
    String maxProductData = "";

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split("/");
      float price = Float.parseFloat(data[1]);

      if (maxPrice < price) {
        maxPrice = price;
        maxProductData = value.toString();
      }
    }

    String[] data = maxProductData.split("/");
    Text newValue = new Text("-> " + data[0] + " " + data[1] + " (" + data[2] + ", " + data[3] + ")");

    outputCollector.collect(key, newValue);
  }
}
