package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    public HeapPage page;
    public int numOfTuples;
    public int currentTuple;

    public HeapPageIterator(HeapPage page) {
        System.out.print("Here");
        this.page = page;
        this.currentTuple = 0;
        this.numOfTuples = this.page.getNumEmptySlots();
    }
        
    public boolean hasNext() {
        return this.currentTuple < this.numOfTuples;
    }
        
    public Tuple next() {
        System.out.print("Inside next");
        return this.page.tuples[this.currentTuple++];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}