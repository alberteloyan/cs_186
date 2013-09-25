package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {

	public Iterator<Tuple> tupleIt;
	public TransactionId tranId;
	public HeapFile itFile;
	public int numPages;
	public int currPageIndex;
	public Page currPage;
	public int fileId;
	

	public HeapFileIterator(TransactionId tid, HeapFile file) {
		tranId = tid;
		itFile = file;
		numPages = file.numPages();
		currPageIndex = 0; 
		currPage = null;
		fileId = itFile.getId();
	}

	public void open()
        throws DbException, TransactionAbortedException {
        HeapPageId hpId = new HeapPageId(this.fileId, 0);
        this.currPage = Database.getBufferPool().getPage(this.tranId, hpId, Permissions.READ_ONLY);
        //System.out.println("potential HeapPage " + new HeapPageId(this.fileId, this.currPageIndex + 1));
        //System.out.println(this.currPage);
		currPageIndex += 1;
        tupleIt = this.currPage.iterator();

    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        if(tupleIt == null)
        	return false;
        if(tupleIt.hasNext())
        	return true;
        
        while(currPageIndex <= (numPages - 1)) {
        	HeapPageId hpId = new HeapPageId(this.fileId, currPageIndex++);
        	currPage = Database.getBufferPool().getPage(this.tranId, hpId, Permissions.READ_ONLY);
        	tupleIt = currPage.iterator();
        	System.out.println("There are numPages = " + numPages + " and we are on " + (currPageIndex - 1));
        	if(tupleIt.hasNext())
        		return true;
        }
        
        return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        //System.out.println(this.currPage);
     	if(tupleIt != null && tupleIt.hasNext()) {
     		 Tuple retTup = tupleIt.next();
     		 //System.out.println(retTup.toString());
     		 if (retTup != null)
     		 	return retTup;
     		else
     			throw new NoSuchElementException();
     	}
     	else
     		throw new NoSuchElementException();   
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
    	this.close();
    	this.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    	currPageIndex = 0;
    	tupleIt = null;
    }

}
