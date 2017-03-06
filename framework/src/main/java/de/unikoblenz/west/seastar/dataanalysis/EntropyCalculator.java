package de.unikoblenz.west.seastar.dataanalysis;

import de.unikoblenz.west.seastar.model.PropertyValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by csarasua.
 */
public class EntropyCalculator {

    //TODO remove from DiversityAnalyzer
    public EntropyCalculator()
    {

    }
    //TODO: cross check total ok?

    public double calculateEntropy(Set<Integer> setOfCounts, int total)
    {
        double entropy=0.0;
        Integer totalI = new Integer(total);

        Set<Double> probabilities = new HashSet<Double>();
        for(Integer i: setOfCounts)
        {
            Integer cI = new Integer(i);
            double prio = cI.doubleValue();
            double totalC = totalI.doubleValue();
            double probability = prio / totalC;
            probabilities.add(probability);
        }

        // If mem query the database

        for (double prob: probabilities)
        {
        //ln
            double log = (Math.log(prob)/ Math.log(2));
            entropy = entropy - (prob * log);
        }
    return entropy;

    }


    public double calculateEntropy(Map<String,Integer> myMap, boolean normalised)
    {
        double entropy=0.0;
        Integer totalI = new Integer(0);



        for(Map.Entry<String,Integer> e: myMap.entrySet())
        {
           totalI = totalI + e.getValue();
        }


        ArrayList<Double> probabilities = new ArrayList<Double>();
        for(Map.Entry<String,Integer> e: myMap.entrySet())
        {

            double prio = e.getValue().doubleValue();
            double totalC = totalI.doubleValue();
            double probability = prio / totalC;
            probabilities.add(probability);
        }



        for (double prob: probabilities)
        {
            //log base 2
            double log = (Math.log(prob)/ Math.log(2.0));
            entropy = entropy - (prob * log);
        }

        if(normalised && myMap.size()!=0 && myMap.size()!=1)
        {
           Integer size = myMap.size();
            double cardinality = size.doubleValue();
            double denominator = (Math.log(cardinality) / Math.log(2.0));

            return (entropy / denominator);

        }
        else
        {
            return entropy;
        }



    }

    public double calculateEntropyP(Map<PropertyValue,Integer> myMap, boolean normalised)
    {
        double entropy=0.0;
        Integer totalI = new Integer(0);


        for(Map.Entry<PropertyValue,Integer> e: myMap.entrySet())
        {
            totalI = totalI + e.getValue();
        }


        ArrayList<Double> probabilities = new ArrayList<Double>();
        for(Map.Entry<PropertyValue,Integer> e: myMap.entrySet())
        {

            double prio = e.getValue().doubleValue();
            double totalC = totalI.doubleValue();
            double probability = prio / totalC;
            probabilities.add(probability);
        }



        for (double prob: probabilities)
        {
        //log base 2
            double log = (Math.log(prob)/ Math.log(2.0));
            entropy = entropy - (prob * log);
        }
        if(normalised && myMap.size()!=0 && myMap.size()!=1)
        {
            Integer size = myMap.size();
            double cardinality = size.doubleValue();
            double denominator = (Math.log(cardinality) / Math.log(2.0));
            return (entropy / denominator);

        }
        else {
            return entropy;
        }

    }




}
