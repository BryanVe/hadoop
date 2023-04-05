package p6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

class CountryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    if (key.get() == 0 || value.toString().contains("Transaction_date")) return;

    String valueString = value.toString();
    String[] rowData = valueString.split(",");

    String[] accountCreatedDate = rowData[8].trim().split(" ");
    String[] dateArray = accountCreatedDate[0].split("/");
    String year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
    String month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
    String day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
    String[] timeArray = accountCreatedDate[1].split(":");
    String hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
    String minutes = timeArray[1];
    String finalDateAccountCreated = year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";

    String[] lastLoginDate = rowData[9].trim().split(" ");
    dateArray = lastLoginDate[0].split("/");
    year = dateArray[2].length() == 2 ? "20" + dateArray[2] : dateArray[2];
    month = dateArray[0].length() == 2 ? dateArray[0] : "0" + dateArray[0];
    day = dateArray[1].length() == 2 ? dateArray[1] : "0" + dateArray[1];
    timeArray = lastLoginDate[1].split(":");
    hours = timeArray[0].length() == 2 ? timeArray[0] : "0" + timeArray[0];
    minutes = timeArray[1];
    String finalDateLogin = year + "-" + month + "-" + day + "T" + hours + ":" + minutes + ":00";

    int antiquity = Math.abs(
      (int) ChronoUnit.DAYS.between(
        LocalDateTime.parse(finalDateLogin),
        LocalDateTime.parse(finalDateAccountCreated)
      )
    );
    String country = rowData[7].trim().toLowerCase();
    String name = rowData[4].trim().toLowerCase();

    String newKeyAndValue = name + "/" + country + "/" + antiquity;

    outputCollector.collect(new Text(newKeyAndValue), new Text(newKeyAndValue));
  }
}
