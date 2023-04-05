package p5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

class CountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");

    String[] transactionDate = rowData[0].trim().split(" ");
    String[] dateArray = transactionDate[0].split("/");
    String year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
    String month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
    String day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
    String[] timeArray = transactionDate[1].split(":");
    String hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
    String minutes = timeArray[1];
    String date = year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";

    String country = rowData[7].trim().toLowerCase();
    String city = rowData[5].trim().toLowerCase();
    String name = rowData[4].trim().toLowerCase();

    String newKeyAndValue = name + "/" + country + "/" + city + "/" + date;

    outputCollector.collect(new Text(newKeyAndValue), new Text(newKeyAndValue));
  }
}
