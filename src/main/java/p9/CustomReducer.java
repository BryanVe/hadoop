package p9;

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
  List<String> readCountry = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] compoundKey = key.toString().split("/");
    String countryKey = compoundKey[1];

    if (!readCountry.contains(countryKey)) {
      readCountry.add(countryKey);

      String name = compoundKey[0];
      String state = compoundKey[2];
      String city = compoundKey[3];
      float price = Float.parseFloat(compoundKey[4]);

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("/");
        String currentName = current[0];
        String currentCountry = current[1];
        String currentState = current[2];
        String currentCity = current[3];
        float currentPrice = Float.parseFloat(current[4]);

        if (countryKey.equals(currentCountry) && currentPrice > price) {
          name = currentName;
          city = currentCity;
          price = currentPrice;
          state = currentState;
        }
      }

      outputCollector.collect(new Text(name), new Text(state + " " + city));
    }
  }
}
