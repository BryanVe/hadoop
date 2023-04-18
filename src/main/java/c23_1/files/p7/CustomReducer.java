package c23_1.files.p7;

import c23_1.files.entities.NameCountry;
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
    List<NameCountry> elements = new ArrayList<>();

    @Override
    public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        String nameKey = key.toString();

        while (iterator.hasNext()) {
            Object value = iterator.next();
            String[] current = value.toString().split("/");
            String currentName = current[0];
            String currentCountry = current[1];
            Integer currentTotal = 0;

            elements.add(new NameCountry(currentName, currentCountry, currentTotal, currentTotal));
        }

        List<NameCountry> elementsFromCurrentPerson = elements.stream()
                .filter(e -> e.getName().equals(nameKey))
                .collect(Collectors.toList());

        // Count how many times a person did a purchase in a country
        List<String> countriesFromCurrentPerson = elementsFromCurrentPerson.stream()
                .map(NameCountry::getCountry)
                .distinct()
                .collect(Collectors.toList());

        List<NameCountry> totalTimesByPerson = new ArrayList<>();

        countriesFromCurrentPerson.forEach(country -> {
            AtomicReference<Integer> totalFromCurrentCard = new AtomicReference<>(0);

            elementsFromCurrentPerson.forEach(e -> {
                if (e.getCountry().equals(country)) totalFromCurrentCard.updateAndGet(v -> v + 1);
            });

           // Add only the lowest total
            if (totalTimesByPerson.stream().noneMatch(e -> e.getCountry().equals(country))) {
                totalTimesByPerson.add(new NameCountry(nameKey, country, totalFromCurrentCard.get(), totalFromCurrentCard.get()));
            } else {
                totalTimesByPerson.stream()
                        .filter(e -> e.getCountry().equals(country))
                        .forEach(e -> {
                            if (e.getTotal() > totalFromCurrentCard.get()) {
                                e.setTotal(totalFromCurrentCard.get());
                            }
                        });
            }
        });

        totalTimesByPerson.forEach(e -> {
            try {
                outputCollector.collect(new Text(e.getName()), new Text(e.getCountry() + "/" + e.getTotal()));
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        });
    }


}
