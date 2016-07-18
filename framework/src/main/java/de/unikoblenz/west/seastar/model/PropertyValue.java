package de.unikoblenz.west.seastar.model;

/**
 * Created by csarasua.
 */
public class PropertyValue {

    String property;
    String value;

    public PropertyValue(String p, String v)
    {
        this.property = p;
        this.value = v;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PropertyValue that = (PropertyValue) o;

        if (!property.equals(that.property)) return false;
        return value.equals(that.value);

    }

    @Override
    public int hashCode() {
        int result = property.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
