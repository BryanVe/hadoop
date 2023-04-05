package p4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> countries = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String countryKey = key.toString();

    if (!countries.contains(countryKey)) {
      countries.add(countryKey);

      int frequency = 0;
      float totalPrice = 0;

      // Convert iterator to a list
      while (iterator.hasNext()) {
        String[] values = iterator.next().toString().split("-");
        String country = values[0];
        float price = Float.parseFloat(values[1]);

        if (country.equals(countryKey)) {
          totalPrice += price;
          frequency++;
        }
      }

      outputCollector.collect(key, new Text("- average: " + String.format("%.2f", totalPrice / frequency)));
    }
  }
}
