package de.unikoblenz.west.seastar.datapreparation;

import java.util.Set;

/**
 * Created by csarasua.
 */
public interface DataSampler {

    public void extractLinksCompleteSample(int sizeOfSample, boolean byType);
    public void extractLinksSample(Set<String> namespacesOfDataset, int sizeOfSample, boolean byType);


}
