package p7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

  public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    double averageLat = 0.0;
    double averageLng = 0.0;
    int i = 0;
    while (values.hasNext()) {
      // replace type of value with the actual type of our value
      String[] vals = values.next().toString().split(",");

      double lat = Double.parseDouble(vals[0]);
      double lng = Double.parseDouble(vals[1]);

      averageLat += lat;
      averageLng += lng;
      i++;
    }
    averageLat = averageLat / i;
    averageLng = averageLng / i;

    output.collect(t_key,
      new Text("avgLat: " + averageLat + ", avgLng: " + averageLng));
  }
}
