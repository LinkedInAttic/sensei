package com.senseidb.search.query.filters;

/**
 * Estimated cardinality of a DocIdSet.
 *
 * We represent estimated cardinality with an uniform distribution from min to
 * max, where 0 is no documents, and 1.0 is all documents.
*/
public class DocIdSetCardinality
    implements Cloneable, Comparable<DocIdSetCardinality> {

  double min;
  double max;

  public static DocIdSetCardinality zero() {
    return new DocIdSetCardinality(0.0, 0.0);
  }

  public static DocIdSetCardinality one() {
    return new DocIdSetCardinality(1.0, 1.0);
  }

  public static DocIdSetCardinality random() {
    return new DocIdSetCardinality(0.0, 1.0);
  }

  public static DocIdSetCardinality exact(double cardinality) {
    return new DocIdSetCardinality(cardinality, cardinality);
  }

  public static DocIdSetCardinality exact(int count, int outOf) {
    return exact(((double)count) / outOf);
  }

  public static DocIdSetCardinality exactRange(int min, int max, int outOf) {
    return new DocIdSetCardinality(((double)min) / outOf, ((double)max) / outOf);
  }

  public static DocIdSetCardinality exactRange(double min, double max) {
    return new DocIdSetCardinality(min, max);
  }

  DocIdSetCardinality(double minCardinality, double maxCardinality) {
    min = Math.min(Math.max(0.0, minCardinality), 1.0);
    max = Math.min(Math.max(0.0, maxCardinality), 1.0);
  }

  public void andWith(DocIdSetCardinality other) {
    min = Math.max(0.0, min + other.min - 1.0);
    max = Math.min(max, other.max);
  }

  public void orWith(DocIdSetCardinality other) {
    min = Math.max(min, other.min);
    max = Math.min(1.0, max + other.max);
  }

  public void invert() {
    final double oldMin = min;
    min = 1.0 - max;
    max = 1.0 - oldMin;
  }

  public boolean isOne() {
    return min >= 1.0 && max >= 1.0;
  }

  public boolean isZero() {
    return min <= 0.0 && max <= 0.0;
  }

  public boolean isRandom() {
    return min <= 0.0 && max >= 1.0;
  }

  @Override public String toString() {
    return min + "-" + max;
  }

  @Override public DocIdSetCardinality clone() {
    try {
      return (DocIdSetCardinality)super.clone();
    } catch (CloneNotSupportedException e) {
      return null;
    }
  }

  // Compare averages: (min + max) / 2, same as comparing min + max
  @Override public int compareTo(DocIdSetCardinality o) {
    return (int)Math.signum(min + max - o.min - o.max);
  }
}
