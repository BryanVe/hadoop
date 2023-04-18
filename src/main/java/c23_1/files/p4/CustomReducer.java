package c23_1.files.p4;

import c23_1.files.utils.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  LocalDateTime now = LocalDateTime.now();


  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    long lastAntiquity = Integer.MAX_VALUE;
    String lastAccountCreated = "";
    String lastName = "";

    while (iterator.hasNext()) {
      Text value = iterator.next();
      String[] data = value.toString().split(",");
      String name = data[0];
      String accountCreated = data[1];
      String accountCreatedUTC = Utils.getUTCDateAsString(accountCreated);
      LocalDateTime parsedAccountCreated = LocalDateTime.parse(accountCreatedUTC);
      System.out.println(accountCreated);

      int currentAntiquity = Math.abs((int) ChronoUnit.DAYS.between(now, parsedAccountCreated));

      if (currentAntiquity < lastAntiquity) {
        lastAntiquity = currentAntiquity;
        lastAccountCreated = accountCreated;
        lastName = name;
      }
    }

    Text newValue = new Text(lastName + " " + lastAccountCreated + " (" + lastAntiquity + " días)");
    outputCollector.collect(key, newValue);
  }
}
