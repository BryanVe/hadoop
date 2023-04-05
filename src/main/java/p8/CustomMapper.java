package p8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

class CustomMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] singleRowData = valueString.split(",");

    String country = singleRowData[7].trim();
    String name = singleRowData[4].trim();
    String accountDate = singleRowData[8].trim();

    if (country.isEmpty() || name.isEmpty() || accountDate.isEmpty()) return;

    SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yy HH:mm");

    try {
      dateFormat.parse(accountDate);
    } catch (ParseException e) {
      return;
    }

    outputCollector.collect(new Text(country + ","), new Text(name + "," + accountDate));
  }
}
