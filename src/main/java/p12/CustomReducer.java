package p12;

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
    String nameKey = key.toString();

    if (nameKey.startsWith("a") || nameKey.startsWith("b")) {
      float maxPrice = Float.MIN_VALUE;
      String maxCard = "";
      // Convert iterator to a list
      while (iterator.hasNext()) {
        String value = iterator.next().toString();
        String[] compoundValue = value.split("/");
        float currentPrice = Float.parseFloat(compoundValue[0]);
        String currentCard = compoundValue[1];
        if (currentPrice > maxPrice) {
          maxPrice = currentPrice;
          maxCard = currentCard;
        }
      }

      outputCollector.collect(new Text(key), new Text(maxCard + " - " + maxPrice));
    }
  }
}
