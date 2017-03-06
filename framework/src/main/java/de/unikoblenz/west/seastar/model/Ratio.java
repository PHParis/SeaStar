package de.unikoblenz.west.seastar.model;

/**
 * Created by csarasua.
 */
public class Ratio {

    double ratioValue;
    int numerator;
    int denominator;

    public Ratio(double  value, int num, int denom)
    {
        this.ratioValue = value;
        this.numerator = num;
        this.denominator = denom;
    }

    public double getRatioValue() {
        return ratioValue;
    }

    public void setRatioValue(double ratioValue) {
        this.ratioValue = ratioValue;
    }

    public int getNumerator() {
        return numerator;
    }

    public void setNumerator(int numerator) {
        this.numerator = numerator;
    }

    public int getDenominator() {
        return denominator;
    }

    public void setDenominator(int denominator) {
        this.denominator = denominator;
    }
}
