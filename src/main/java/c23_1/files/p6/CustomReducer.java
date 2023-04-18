package c23_1.files.p6;

import c23_1.files.entities.NameCountryPrice;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        List<NameCountryPrice> elements = new ArrayList<>();

        // Convert iterator to a list
        while (iterator.hasNext()) {
            Object value = iterator.next();
            String[] current = value.toString().split("/");
            String currentName = current[0];
            String currentCountry = current[1];
            float currentPrice = Float.parseFloat(current[2]);

            elements.add(new NameCountryPrice(currentName, currentCountry, currentPrice));
        }

        // Get unique countries and save in a list
        List<String> countries = elements.stream()
                .map(NameCountryPrice::getCountry)
                .distinct()
                .collect(Collectors.toList());

        // Get only the highest price for each country if equals add both
        List<NameCountryPrice> highestPricePerCountry = new ArrayList<>();
        countries.forEach(country -> {
            AtomicReference<Float> highestPrice = new AtomicReference<>((float) 0);

            // Find the highest price
            elements.forEach(e -> {
                if (e.getCountry().equals(country) && e.getPrice() > highestPrice.get()) {
                    highestPrice.set(e.getPrice());
                }
            });

            // Find other elements with the same price
            elements.forEach(e -> {
                if (e.getCountry().equals(country) && e.getPrice() == highestPrice.get()) {
                    highestPricePerCountry.add(e);
                }
            });

        });

        highestPricePerCountry.forEach(e -> {
            try {
                outputCollector.collect(new Text(e.getCountry()), new Text(e.getName() + "/" + e.getPrice()));
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });


    }
}
