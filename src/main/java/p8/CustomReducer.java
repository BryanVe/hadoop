package p8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  @Override
  public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    String recently = "";
    Date recentlyDate = new Date(Long.MIN_VALUE);

    while (values.hasNext()) {
      String[] singleRowData = values.next().toString().split(",");

      SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm");

      Date date;

      try {
        date = dateFormat.parse(singleRowData[1]);
      } catch (ParseException e) {
        return;
      }

      if (date.compareTo(recentlyDate) > 0) {
        recently = singleRowData[0];
        recentlyDate = date;
      }
    }

    output.collect(key, new Text(recently));
  }
}
