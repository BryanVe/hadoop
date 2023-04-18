package c23_1.files.p2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.*;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, FloatWritable> {
  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, FloatWritable> outputCollector, Reporter reporter) throws IOException {
    // create map for saving payment types with its total prices
    HashMap<String, Float> prices = new HashMap<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split("/");
      String paymentType = data[0];
      float price = Float.parseFloat(data[1]);

      // fill the prices map
      if (prices.containsKey(paymentType)) {
        float currentTotal = prices.get(paymentType);
        prices.put(paymentType, currentTotal + price);
      } else {
        prices.put(paymentType, price);
      }
    }

    // iterate over map and get the max total price
    Map.Entry<String, Float> maxCard = prices.entrySet().iterator().next();

    for (Map.Entry<String, Float> entry : prices.entrySet()) {
      if (entry.getValue().compareTo(maxCard.getValue()) > 0) {
        maxCard = entry;
      }
    }

    String state = key.toString();
    String maxPaymentCard = maxCard.getKey();
    Float maxTotal = maxCard.getValue();
    Text newKey = new Text(state + " " + maxPaymentCard);

    outputCollector.collect(newKey, new FloatWritable(maxTotal));
  }
}
