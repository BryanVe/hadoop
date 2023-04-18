package c23_1.files.entities;

public class NameCountry {
    private final String name;
    private final String country;
    private final Integer currentTotal;

    public NameCountry(String name, String country, Integer integer, Integer currentTotal) {
        this.name = name;
        this.country = country;
        this.currentTotal = currentTotal;
    }

    public String getName() {
        return name;
    }

    public String getCountry() {
        return country;
    }

    public Integer getTotal() {
        return currentTotal;
    }

    public  Integer setTotal(Integer total) {
        return currentTotal;
    }
}
