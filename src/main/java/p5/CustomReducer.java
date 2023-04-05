package p5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readCountry = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String[] compoundKey = key.toString().split("/");
    String countryKey = compoundKey[1];
    String dateKey = compoundKey[3];
    LocalDateTime maxDate = LocalDateTime.parse(dateKey);

    if (!readCountry.contains(countryKey)) {
      readCountry.add(countryKey);

      String name = compoundKey[0];
      String city = compoundKey[2];

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("/");
        String currentName = current[0];
        String currentCountry = current[1];
        String currentCity = current[2];
        LocalDateTime currentTransactionDate = LocalDateTime.parse(current[3]);

        if (countryKey.equals(currentCountry) && maxDate.compareTo(currentTransactionDate) < 0) {
          name = currentName;
          city = currentCity;
          maxDate = currentTransactionDate;
        }
      }

      outputCollector.collect(new Text(name), new Text(city));
    }
  }
}
