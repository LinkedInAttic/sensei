package com.senseidb.search.query.filters;

/**
 * Estimated cardinality of a DocIdSet.
 *
 * We represent estimated cardinality with an uniform distribution from min to
 * max, where 0 is no documents, and 1.0 is all documents.
 *
 * Cardinality is used during query planning. ANDs and ORs short-circuit. If
 * one test in an AND fails, others are not evaluated. If one test in an OR
 * matches, others are not evaluated. We therefore want to re-order ANDs by
 * increasing cardinality (most-selective first), and ORs by decreasing. We
 * also remove match-all (cardinality 1.0-1.0) terms from ANDs, and match-none
 * (cardinality 0.0-0.0) terms from ORs.
*/
public class DocIdSetCardinality
    implements Cloneable, Comparable<DocIdSetCardinality> {

  double min;
  double max;

  /**
   * Match-none cardinality, matches 0 docs.
   */
  public static DocIdSetCardinality zero() {
    return new DocIdSetCardinality(0.0, 0.0);
  }

  /**
   * Match-all cardinality, matches all docs.
   */
  public static DocIdSetCardinality one() {
    return new DocIdSetCardinality(1.0, 1.0);
  }

  /**
   * Unspecified cardinality, matching anywhere from zero to all docs.
   */
  public static DocIdSetCardinality random() {
    return new DocIdSetCardinality(0.0, 1.0);
  }

  /**
   * Exact cardinality, matches the given ratio of docs.
   *
   * @param cardinality Ratio (0.0-1.0).
   */
  public static DocIdSetCardinality exact(double cardinality) {
    return new DocIdSetCardinality(cardinality, cardinality);
  }

  /**
   * Exact cardinality, matches the given number of docs.
   *
   * @param count Number of matched docs (0-outOf).
   * @param outOf Total number of docs.
   */
  public static DocIdSetCardinality exact(int count, int outOf) {
    return exact(((double)count) / outOf);
  }

  /**
   * Exact cardinality range, matching from min to max ratio.
   *
   * @param min Minimum ratio (0.0-1.0)
   * @param max Minimum ratio (0.0-1.0)
   */
  public static DocIdSetCardinality exactRange(double min, double max) {
    return new DocIdSetCardinality(min, max);
  }

  /**
   * Exact cardinality range, matching from min to max docs.
   *
   * @param min Minimum number of matched docs (0-outOf).
   * @param max Maximum number of matched docs (0-outOf).
   * @param outOf Total number of docs.
   */
  public static DocIdSetCardinality exactRange(int min, int max, int outOf) {
    return new DocIdSetCardinality(((double)min) / outOf, ((double)max) / outOf);
  }

  /**
   * Default constructor - do not use.
   *
   * Use named static factory functions instead.
   */
  DocIdSetCardinality(double minCardinality, double maxCardinality) {
    min = Math.min(Math.max(0.0, minCardinality), 1.0);
    max = Math.min(Math.max(0.0, maxCardinality), 1.0);
  }

  /**
   * AND with another cardinality.
   *
   * Example: Presuming 100 docs, one query matches 80-90, the other 70-80.
   * Result can match no less than 50, presuming the first query matched 80
   * (didn't match 20) and the second query matched 70. It cannot match more
   * than 80 (the smaller of two maxes)
   */
  public void andWith(DocIdSetCardinality other) {
    min = Math.max(0.0, min + other.min - 1.0); // min - (1.0 - other.min)
    max = Math.min(max, other.max);
  }

  /**
   * OR with another cardinality.
   *
   * Example: Presuming 100 docs, one query matches 0-10, the other 20-30.
   * Result can match no less than 20 (the larger of two). It cannot match
   * more than 40 (the sum).
   */
  public void orWith(DocIdSetCardinality other) {
    min = Math.max(min, other.min);
    max = Math.min(1.0, max + other.max);
  }

  /**
   * Invert this cardinality.
   *
   * Example: if one query matches 50%-100% of documents, it's inverse matches
   * 0%-50%.
   */
  public void invert() {
    final double oldMin = min;
    min = 1.0 - max;
    max = 1.0 - oldMin;
  }

  /**
   * @return True if this query matches all docs.
   */
  public boolean isOne() {
    return min >= 1.0 && max >= 1.0;
  }

  /**
   * @return True if this query matches no docs.
   */
  public boolean isZero() {
    return min <= 0.0 && max <= 0.0;
  }

  /**
   * @return True if we have no cardinality info.
   */
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

  /**
   * Default comparator.
   *
   * Compares medians: (min + max)/2.
   */
  @Override public int compareTo(DocIdSetCardinality o) {
    return (int)Math.signum(min + max - o.min - o.max);
  }
}
