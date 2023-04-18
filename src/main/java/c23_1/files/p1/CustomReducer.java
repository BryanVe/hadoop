package c23_1.files.p1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
  @Override
  public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
    // create map for saving payment types with its total prices
    int frequency = 0;

    while (iterator.hasNext()) {
      IntWritable value = iterator.next();
      frequency += value.get();
    }

    outputCollector.collect(key, new IntWritable(frequency));
  }
}
