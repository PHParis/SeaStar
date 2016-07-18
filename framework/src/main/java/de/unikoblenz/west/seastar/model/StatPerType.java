package de.unikoblenz.west.seastar.model;

/**
 * Created by csarasua.
 */
public class StatPerType {

    String type;
    double statValueDou;
    int statValueInt;

    public StatPerType(String type, double valueD, int valueI)
    {
        this.type = type;
        this.statValueDou = valueD;
        this.statValueInt = valueI;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getStatValueDou() {
        return statValueDou;
    }

    public void setStatValueDou(double statValueDou) {
        this.statValueDou = statValueDou;
    }

    public int getStatValueInt() {
        return statValueInt;
    }

    public void setStatValueInt(int statValueInt) {
        this.statValueInt = statValueInt;
    }
}
