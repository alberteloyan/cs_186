package simpledb;

import simpledb.Predicate.Op;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private int numBuckets;
	private int min;
	private int max;
	private int[] buckets;
	private int bucketSize;
	private int valCount;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.numBuckets = buckets;
    	this.buckets = new int[buckets];
    	this.min = min;
    	this.max = max;
    	this.bucketSize = (int)Math.ceil((double)(max - min) / buckets);
    	this.valCount = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	for(int i = 0; i < this.numBuckets; i++) {
    		if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
    			buckets[i] = buckets[i] + 1;
    			this.valCount = this.valCount + 1;
    			return;
    		}
    	}
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	boolean bucketHit = false;
        double fract = 0.0;
        
    	if(op.equals(Op.EQUALS)) {
     		for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					fract = (double)(buckets[i]/bucketSize)/valCount;
					return fract;
				}
			}
			return fract;
        }
        if(op.equals(Op.NOT_EQUALS)) {
         	for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					fract = 1 - (double)(buckets[i]/bucketSize)/valCount;
					return fract;
				}
			}
			return 1 - fract;
        }
        if(op.equals(Op.GREATER_THAN) ) {
        	if(v >= max) 
        		return 0.0;
        	if(v < min)
        		return 1.0;
        		
           	for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					bucketHit = true;
					fract += (double)(((min + (i + 1) * this.bucketSize) - v) / this.bucketSize) * (buckets[i]/valCount) - (buckets[i] / valCount);
				}
				if (bucketHit) {
					fract += (double)buckets[i]/valCount;
				}
			}
			return fract;
        }
        if(op.equals(Op.GREATER_THAN_OR_EQ) ) {
        	if(v >= max) 
        		return 0.0;
        	if(v < min)
        		return 1.0;
      	 	for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					bucketHit = true;
				}
				if (bucketHit) {
					fract += (double)buckets[i]/valCount;
				}
			}
			return fract;
        }
        if(op.equals(Op.LESS_THAN)) {
        	if(v > max) 
        		return 1.0;
        	if(v <= min)
        		return 0.0;
          	for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					bucketHit = true;
					
					fract += (double)((v - (min + i * this.bucketSize)) / this.bucketSize) * (buckets[i]/valCount);
				}
				if (!bucketHit) {
					fract += (double)buckets[i]/valCount;
				}
			}
			return fract;
        }
        if(op.equals(Op.LESS_THAN_OR_EQ)) {
        	if(v > max) 
        		return 1.0;
        	if(v <= min)
        		return 0.0;
         	for(int i = 0; i < this.numBuckets; i++) {
				if((v < (min + (i+1)*this.bucketSize)) && (v >= (min + i * this.bucketSize))) {
					bucketHit = true;
					fract += (double)buckets[i]/valCount;
				}
				if (!bucketHit) {
					fract += (double)buckets[i]/valCount;
				}
			}
			return fract;
        }
        
        return 0.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
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
