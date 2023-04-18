package c23_1.files.entities;

public class NameCountryPrice {
    private final String name;
    private final String country;
    private final float price;

    public NameCountryPrice(String name, String country, float price) {
        this.name = name;
        this.country = country;
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    public float getPrice() {
        return price;
    }
}
