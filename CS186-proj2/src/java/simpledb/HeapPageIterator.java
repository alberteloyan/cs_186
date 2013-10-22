package simpledb;

import java.util.*;

public class HeapPageIterator implements Iterator<Tuple> {
    public HeapPage page;
    public int tupleNum;
    public int currTuple;

    public HeapPageIterator(HeapPage page) {
        //System.out.print("Here");
        this.page = page;
        this.currTuple = 0;
        int openTuples = this.page.getNumTuples() - this.page.getNumEmptySlots();
        this.tupleNum = openTuples;
    }
        
    public boolean hasNext() {
        return this.currTuple < this.tupleNum;
    }
        
    public Tuple next() {
        //System.out.print("Inside next");
        return this.page.tuples[this.currTuple++];
    }
        
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Cannot remove on HeapPageIterator");
    }
}
