package de.unikoblenz.west.seastar.model;

/**
 * Created by csarasua.
 */
public class Count {

    String id;
    int countValue;

    public Count(String id, int value)
    {
        this.id = id;
        this.countValue = value;

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getCountValue() {
        return countValue;
    }

    public void setCountValue(int countValue) {
        this.countValue = countValue;
    }
}
