package p2;

class CountryCardPrice {
  private final String country;
  private final String card;
  private final float price;

  public CountryCardPrice(String country, String card, float price) {
    this.country = country;
    this.card = card;
    this.price = price;
  }

  public String getCountry() {
    return country;
  }

  public String getCard() {
    return card;
  }

  public float getPrice() {
    return price;
  }
}
