package de.unikoblenz.west.seastar.model;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
/**
 * @author csarasua
 */
public class Link {

    private String id;

    private Resource elementA;
    private Resource elementB;
    private Property relation;

    private double measure;

    private boolean invented;

    public Link(Resource elemA, Resource elemB, Property rel, double m) {
        this.elementA = elemA;
        this.elementB = elemB;
        this.relation = rel;
        this.measure = m;

        this.id = elemA.getLocalName() +  relation.getLocalName()+ elemB.getLocalName();
        this.invented = false;

    }

    public Link(Resource elemA, Resource elemB, Property rel) {
        this.elementA = elemA;
        this.elementB = elemB;
        this.relation = rel;

        this.id = elemA.getLocalName() +  relation.getLocalName()+ elemB.getLocalName();
        this.invented = false;
    }

    public boolean isInvented() {
        return invented;
    }

    public void setInvented(boolean invented) {
        this.invented = invented;
    }

    public Resource getElementA() {
        return elementA;
    }

    public void setElementA(Resource elementA) {
        this.elementA = elementA;
    }

    public Resource getElementB() {
        return elementB;
    }

    public void setElementB(Resource elementB) {
        this.elementB = elementB;
    }

    public Property getRelation() {

        return this.relation;
    }

    public void setRelation(Property relation) {
        this.relation = relation;
    }

    public String getMeasure() {


        return String.valueOf(measure);
    }

    public void setMeasure(double measure) {
        this.measure = measure;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Link link = (Link) o;

        if (!elementA.equals(link.elementA)) return false;
        if (!elementB.equals(link.elementB)) return false;
        if (!id.equals(link.id)) return false;
        if (!relation.equals(link.relation)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + elementA.hashCode();
        result = 31 * result + elementB.hashCode();
        result = 31 * result + relation.hashCode();
        return result;
    }
}
