package c23_1.files.p8;

import c23_1.files.entities.CountryCity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        List<CountryCity> elements = new ArrayList<>();
        HashMap<String, Integer> frequencies = new HashMap<>();

        // Get the size of elements array
        while (iterator.hasNext()) {
            Object value = iterator.next();
            String[] current = value.toString().split("/");
            String currentCountry = current[1];
            String currentCity = current[0];

            elements.add(new CountryCity(currentCountry, currentCity));
        }

        // from elements list only save the uniques cities
        List<String> cities = elements.stream()
                .map(CountryCity::getCity)
                .distinct()
                .collect(Collectors.toList());

        frequencies.put(text.toString(), cities.size());

        outputCollector.collect(new Text(text.toString()), new Text(cities.size() + ""));

    }

}
