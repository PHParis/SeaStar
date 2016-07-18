package de.unikoblenz.west.seastar.model;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.Map;
import java.util.Set;
/**
 * @author csarasua
 * Keeps all links and multiple indexes of them to be able to access them in different ways (e.g. per target data set, per predicate etc.),
 */
public class Interlinking {

    private Dataset dataset1;
    private Set<Link> setOfLinks;

    Set<Linkset> setOfLinksets;

    // TODO: populate link indexes of Interlinking
    Map<Dataset, Set<Link>> indexLinkPerDataset;
    Map<Resource, Set<Link>> indexLinkPerEntity;
    Map<Property, Set<Link>> indexLinkPerPredicate;


    public Interlinking(Dataset d1,Set<Link> setL) {
        this.dataset1 = d1;
        this.setOfLinks = setL;
    }


    public Dataset getDataset1() {
        return dataset1;
    }

    public void setDataset1(Dataset dataset1) {
        this.dataset1 = dataset1;
    }


    public Set<Link> getSetOfLinks() {
        return setOfLinks;
    }

    public void setSetOfLinks(Set<Link> setOfLinks) {
        this.setOfLinks = setOfLinks;
    }

    public Set<Linkset> getSetOfLinksets() {
        return setOfLinksets;
    }

    public void setSetOfLinksets(Set<Linkset> setOfLinksets) {
        this.setOfLinksets = setOfLinksets;
    }

    public Map<Dataset, Set<Link>> getIndexLinkPerDataset() {
        return indexLinkPerDataset;
    }

    public void setIndexLinkPerDataset(Map<Dataset, Set<Link>> indexLinkPerDataset) {
        this.indexLinkPerDataset = indexLinkPerDataset;
    }

    public Map<Resource, Set<Link>> getIndexLinkPerEntity() {
        return indexLinkPerEntity;
    }

    public void setIndexLinkPerEntity(Map<Resource, Set<Link>> indexLinkPerEntity) {
        this.indexLinkPerEntity = indexLinkPerEntity;
    }

    public Map<Property, Set<Link>> getIndexLinkPerPredicate() {
        return indexLinkPerPredicate;
    }

    public void setIndexLinkPerPredicate(Map<Property, Set<Link>> indexLinkPerPredicate) {
        this.indexLinkPerPredicate = indexLinkPerPredicate;
    }
}
