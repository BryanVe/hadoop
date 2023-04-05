package p2;

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
  List<String> readCountries = new ArrayList<>();

  @Override
  public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
    String countryKey = key.toString();

    if (!readCountries.contains(countryKey)) {
      readCountries.add(countryKey);

      List<CountryCardPrice> elements = new ArrayList<>();

      // Convert iterator to a list
      while (iterator.hasNext()) {
        Object value = iterator.next();
        String[] current = value.toString().split("-");
        String currentCountry = current[0];
        String currentCard = current[1];
        float currentPrice = Float.parseFloat(current[2]);

        elements.add(new CountryCardPrice(currentCountry, currentCard, currentPrice));
      }

      List<CountryCardPrice> elementsFromCurrentCountry = elements
        .stream()
        .filter(e -> {
          String elementCountry = e.getCountry();

          return elementCountry.equals(countryKey);
        })
        .collect(Collectors.toList());
      List<String> cardsFromCurrentCountry = elementsFromCurrentCountry
        .stream()
        .map(CountryCardPrice::getCard)
        .distinct()
        .collect(Collectors.toList());
      List<CountryCardPrice> totalSpentByCard = new ArrayList<>();

      cardsFromCurrentCountry.forEach(card -> {
        AtomicReference<Float> totalFromCurrentCard = new AtomicReference<>((float) 0);

        elementsFromCurrentCountry.forEach(e -> {
          if (e.getCard().equals(card)) totalFromCurrentCard.updateAndGet(v -> v + e.getPrice());
        });
        totalSpentByCard.add(new CountryCardPrice(countryKey, card, totalFromCurrentCard.get()));
      });

      float max = 0;
      String maxCard = "";

      for (CountryCardPrice e : totalSpentByCard)
        if (e.getPrice() > max) {
          max = e.getPrice();
          maxCard = e.getCard();
        }

      // Add the current country to the list of countries to avoid collect it again
      String newValue = maxCard + " " + max;

      outputCollector.collect(new Text(countryKey), new Text(newValue));
    }
  }
}
