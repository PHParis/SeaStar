package de.unikoblenz.west.seastar.model;

/**
 * Created by csarasua.
 */
public class Linkset {

    Dataset d1;
    Dataset d2;

    String predicate;
    int countLinks;

    public Linkset(Dataset d1, Dataset d2, String pred, int links)
    {
        this.d1=d1;
        this.d2=d2;

        this.predicate=pred;
        this.countLinks=links;
    }

    public Dataset getD1() {
        return d1;
    }

    public void setD1(Dataset d1) {
        this.d1 = d1;
    }

    public Dataset getD2() {
        return d2;
    }

    public void setD2(Dataset d2) {
        this.d2 = d2;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public int getCountLinks() {
        return countLinks;
    }

    public void setCountLinks(int countLinks) {
        this.countLinks = countLinks;
    }
}
