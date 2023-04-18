package c23_1.files.p3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    // create map for saving frequencies of payment types
    HashMap<String, Integer> frequencies = new HashMap<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String paymentType = value.toString();

      // fill the frequencies map
      if (frequencies.containsKey(paymentType)) {
        int currentTotal = frequencies.get(paymentType);
        frequencies.put(paymentType, currentTotal + 1);
      } else {
        frequencies.put(paymentType, 1);
      }
    }

    // iterate over map and get the max total price
    Map.Entry<String, Integer> maxPaymentType = frequencies.entrySet().iterator().next();
    Map.Entry<String, Integer> minPaymentType = frequencies.entrySet().iterator().next();

    for (Map.Entry<String, Integer> entry : frequencies.entrySet()) {
      if (entry.getValue().compareTo(maxPaymentType.getValue()) > 0) {
        maxPaymentType = entry;
      }
      if (entry.getValue().compareTo(maxPaymentType.getValue()) < minPaymentType.getValue()) {
        minPaymentType = entry;
      }
    }

    String maxPaymentCard = maxPaymentType.getKey();
    Integer maxFrequency = maxPaymentType.getValue();
    String minPaymentCard = minPaymentType.getKey();
    Integer minFrequency = minPaymentType.getValue();
    Text newValue = new Text(maxPaymentCard + " (" + maxFrequency + ") " + minPaymentCard + " (" + minFrequency + ")");

    outputCollector.collect(key, newValue);
  }
}
