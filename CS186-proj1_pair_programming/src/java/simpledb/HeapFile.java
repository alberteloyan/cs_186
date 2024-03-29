package simpledb;

import java.io.*;
import java.util.*;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	public File heapFileBacking;
	public TupleDesc heapFileTD; 
	public int uniqId;
	public FileChannel fc;
	public int pageSize;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.heapFileBacking = f;
        this.heapFileTD = td;
        this.uniqId = f.getAbsoluteFile().hashCode();
        this.pageSize = BufferPool.PAGE_SIZE;
        
        try {
        	RandomAccessFile randFileAccess = new RandomAccessFile(f, "rw");
        	fc = randFileAccess.getChannel();
        } catch (IOException error) {
        	System.out.println(error);
        	System.exit(1);
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.heapFileBacking;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.uniqId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.heapFileTD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageNum = pid.pageNumber();
        int offset = this.pageSize * pageNum;
        try {
		    ByteBuffer byteBuff = ByteBuffer.allocate(this.pageSize);
		    fc.read(byteBuff, offset);
		    HeapPageId retHPId = ((HeapPageId)pid);
		    HeapPage retHP = new HeapPage(retHPId, byteBuff.array());
		    return retHP;
		} catch (IOException error) {
			System.out.println(error);
			System.exit(1);
			return null;
		}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        try {
        	int num = ((int)(this.fc.size()/this.pageSize));
        	return num;
        } catch (IOException error) {
        	System.out.println(error);
        	System.exit(1);
        	return -1;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        
        return new HeapFileIterator(tid, this);
    }

}

