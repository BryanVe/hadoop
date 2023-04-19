package c23_1.files.p14;

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
    HashMap<String, HashMap<String, Integer>> prices = new HashMap<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split("/");
      String paymentType = data[0];
      int price = Integer.parseInt(data[1]);

      if (prices.containsKey(paymentType)) {
        HashMap<String, Integer> element = prices.get(paymentType);
        int total = element.get("total");
        int counter = element.get("counter");
        HashMap<String, Integer> newValue = new HashMap<>();
        newValue.put("total", total + price);
        newValue.put("counter", counter + 1);
        prices.put(paymentType, newValue);
      } else {
        HashMap<String, Integer> newValue = new HashMap<>();
        newValue.put("total", price);
        newValue.put("counter", 1);
        prices.put(paymentType, newValue);
      }
    }

    // iterate over map and get the max total price
    Map.Entry<String, HashMap<String, Integer>> maxPaymentType = prices.entrySet().iterator().next();
    Map.Entry<String, HashMap<String, Integer>> minPaymentType = prices.entrySet().iterator().next();

    for (Map.Entry<String, HashMap<String, Integer>> entry : prices.entrySet()) {
      if (entry.getValue().get("total").compareTo(maxPaymentType.getValue().get("total")) > 0) {
        maxPaymentType = entry;
      }
      if (entry.getValue().get("total").compareTo(maxPaymentType.getValue().get("total")) < minPaymentType.getValue().get("total")) {
        minPaymentType = entry;
      }
    }

    String maxPaymentCard = maxPaymentType.getKey();
    int maxTotal = maxPaymentType.getValue().get("total");
    int maxCounter = maxPaymentType.getValue().get("counter");
    String minPaymentCard = minPaymentType.getKey();
    int minTotal = minPaymentType.getValue().get("total");
    int minCounter = minPaymentType.getValue().get("counter");

    System.out.println(key.toString());
    System.out.println(minPaymentCard);
    System.out.println(minTotal);

    Text newValue = new Text(maxPaymentCard + " (" + maxTotal + ", " + maxCounter + ") " + minPaymentCard + " (" + minTotal + ", " + minCounter + ")");
    outputCollector.collect(key, newValue);
  }
}
