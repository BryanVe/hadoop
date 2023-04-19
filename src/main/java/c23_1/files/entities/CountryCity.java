package c23_1.files.entities;

public class CountryCity {
    private String country;
    private String city;

    public CountryCity(String country, String city) {
        this.country = country;
        this.city = city;
    }

    public CountryCity() {
    }

    public String getCountry() {
        return country;
    }

    public String getCity() {
        return city;
    }
}
