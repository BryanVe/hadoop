package c23_1.files.p5;

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
    // create map for saving frequencies of transactions
    HashMap<String, Integer> frequencies = new HashMap<>();

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String state = value.toString();

      // fill the frequencies map
      if (frequencies.containsKey(state)) {
        int currentTotal = frequencies.get(state);
        frequencies.put(state, currentTotal + 1);
      } else {
        frequencies.put(state, 1);
      }
    }

    // iterate over map and get the max total price
    Map.Entry<String, Integer> maxTransactions = frequencies.entrySet().iterator().next();

    for (Map.Entry<String, Integer> entry : frequencies.entrySet()) {
      if (entry.getValue().compareTo(maxTransactions.getValue()) > 0) {
        maxTransactions = entry;
      }
    }

    String maxState = maxTransactions.getKey();
    Integer maxFrequency = maxTransactions.getValue();
    Text newValue = new Text(maxState + " (" + maxFrequency + ")");

    outputCollector.collect(key, newValue);
  }
}
