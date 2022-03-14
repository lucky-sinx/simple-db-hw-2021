package simpledb.optimizer;

import simpledb.execution.Predicate;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int bucketCnt;
    private int min;
    private int max;
    private int bucketSize;
    private int[] bucketList;
    private int[] bucketSum;
    private int tupleCnt;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.bucketCnt = buckets;
        this.min = min;
        this.max = max;
        this.tupleCnt = 0;
        this.bucketList = new int[buckets];
        this.bucketSum = new int[buckets];
        this.bucketSize = (int) Math.ceil(1.0 * (max - min + 1) / bucketCnt);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        tupleCnt++;
        int index = getTheIndexOfBuckets(v);
        bucketList[index]++;
    }

    public int getTheIndexOfBuckets(int val) {
        int index = (val - min) / bucketSize;
        return index;
    }

    public int getBiasOfBucket(int val) {
        return (val - min) % bucketSize;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        for (int i = 0; i < bucketCnt; i++) {
            if (i == 0) {
                bucketSum[i] = bucketList[i];
            } else {
                bucketSum[i] = bucketSum[i - 1] + bucketList[i];
            }
        }
        double res = 1.0;
        double beforeSum, afterSum;
        int bias;
        int index = getTheIndexOfBuckets(v);
        switch (op) {
            case EQUALS:
                if(v<min||v>max)return 0;
                res = 1.0 * bucketList[index] / bucketSize / tupleCnt;
                break;
            case NOT_EQUALS:
                if(v<min||v>max)return 1;
                res = 1.0 - 1.0 * bucketList[index] / bucketSize / tupleCnt;
                break;
            case LESS_THAN:
                if(v<min)return 0;
                if(v>max)return 1;
                beforeSum = index == 0 ? 0 : bucketSum[index - 1] * 1.0 / tupleCnt;
                bias = getBiasOfBucket(v);
                res = beforeSum + 1.0 * bias * bucketList[index] / bucketSize / tupleCnt;
                break;
            case LESS_THAN_OR_EQ:
                if(v<min)return 0;
                if(v>max)return 1;
                beforeSum = index == 0 ? 0 : bucketSum[index - 1] * 1.0 / tupleCnt;
                bias = getBiasOfBucket(v) + 1;
                res = beforeSum + 1.0 * bias * bucketList[index] / bucketSize / tupleCnt;
                break;
            case GREATER_THAN:
                if(v<min)return 1;
                if(v>max)return 0;
                afterSum = 1.0 * (tupleCnt - bucketSum[index]) / tupleCnt;
                bias = bucketSize - getBiasOfBucket(v) - 1;
                res = afterSum + 1.0 * bias * bucketList[index] / bucketSize / tupleCnt;
                break;
            case GREATER_THAN_OR_EQ:
                if(v<min)return 1;
                if(v>max)return 0;
                afterSum = 1.0 * (tupleCnt - bucketSum[index]) / tupleCnt;
                bias = bucketSize - getBiasOfBucket(v);
                res = afterSum + 1.0 * bias * bucketList[index] / bucketSize / tupleCnt;
                break;
        }
        return res;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
