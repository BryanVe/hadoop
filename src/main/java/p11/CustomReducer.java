package p11;

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
    String[] compoundKey = key.toString().split("/");
    String nameKey = compoundKey[0];
    String countryKey = compoundKey[1];

    if (nameKey.startsWith("j") && countryKey.endsWith("s")) {
      int pricesQuantity = 0;
      float sumPrices = 0;
      // Convert iterator to a list
      while (iterator.hasNext()) {
        float currentPrice = Float.parseFloat(iterator.next().toString());
        sumPrices += currentPrice;
        pricesQuantity++;
      }

      outputCollector.collect(new Text(key), new Text(Float.toString(sumPrices / pricesQuantity)));
    }
  }
}
