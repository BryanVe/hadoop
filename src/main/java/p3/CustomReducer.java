package p3;

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

class CustomReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
  List<String> readPeople = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String nameKey = key.toString();

    if (!readPeople.contains(nameKey)) {
      readPeople.add(nameKey);

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

      List<NameCountryPrice> elementsFromCurrentPerson = elements
        .stream()
        .filter(e -> {
          String elementName = e.getName();

          return elementName.equals(nameKey);
        })
        .collect(Collectors.toList());
      List<String> countriesFromCurrentPerson = elementsFromCurrentPerson
        .stream()
        .map(NameCountryPrice::getCountry)
        .distinct()
        .collect(Collectors.toList());
      List<NameCountryPrice> totalSpentByPerson = new ArrayList<>();

      countriesFromCurrentPerson.forEach(country -> {
        AtomicReference<Float> totalFromCurrentCard = new AtomicReference<>((float) 0);

        elementsFromCurrentPerson.forEach(e -> {
          if (e.getCountry().equals(country)) totalFromCurrentCard.updateAndGet(v -> v + e.getPrice());
        });
        totalSpentByPerson.add(new NameCountryPrice(nameKey, country, totalFromCurrentCard.get()));
      });
      //System.out.println(totalSpentByPerson);

      float max = 0;
      String maxCountry = "";

      for (NameCountryPrice e : totalSpentByPerson)
        if (e.getPrice() > max) {
          max = e.getPrice();
          maxCountry = e.getCountry();
        }

      // Add the current country to the list of countries to avoid collect it again
      String newValue = maxCountry + " " + max;

      outputCollector.collect(new Text(nameKey), new Text(newValue));
    }
  }
}
