package c23_1.files.p9;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String southValue = "";
        float southLatitude = Float.MAX_VALUE;

        while (iterator.hasNext()) {
            Text value = iterator.next();
            String[] data = value.toString().split("/");
            float latitude = Float.parseFloat(data[1]);

            if (latitude < southLatitude) {
                southLatitude = latitude;
                southValue = value.toString();
            }
        }

        String[] data = southValue.split("/");
        String southName = data[0];
        String southLongitude = data[2];
        Text newValue = new Text(southName + " (" + southLatitude + ", " + southLongitude + ")");

        outputCollector.collect(key, newValue);
    }
}
