package simpledb;

import java.io.*;
import java.util.*;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
	public int pageNum;
    public HashMap<PageId, Page> cache;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pageNum = numPages;
        this.cache = new HashMap<PageId, Page>();
    }

    private boolean isBufferFull() {

        return this.cache.size() >= this.pageNum;
    }

    private boolean isPageInCache(PageId pid) {
        return this.cache.containsKey(pid);
    }

    private void bumpPage(PageId pid) {
        if (this.cache.get(pid.hashCode()) != null) {
            this.cache.remove(pid);
        }
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
		if(!isPageInCache(pid)) {
			if (this.isBufferFull()) {
				this.evictPage();
			}
			DbFile dbf = Database.getCatalog().getDbFile(pid.getTableId());
			Page page = dbf.readPage(pid);
			page.markDirty(false, tid);
			cache.put(pid, page);
			
		}
        return this.cache.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        //HeapPage hp = this.getPage(tid, t.getRecordId().getPageId());
        HeapFile f = (HeapFile)Database.getCatalog().getDbFile(tableId);
        HeapPage pg = (HeapPage)f.insertTuple(tid, t).get(0);
        pg.markDirty(true, tid);
        cache.put(pg.getId(), pg);
        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        
        int tableId = t.getRecordId().getPageId().getTableId();
    	DbFile f = Database.getCatalog().getDbFile(tableId);
    	f.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj1
        //pull full set of keys from cache and flush each of them individually
        //Set<PageId> ids = this.cache.keySet();
        for(PageId id : this.cache.keySet()) {
        	this.flushPage(id);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
		// not necessary for proj1
		this.cache.remove(pid);
		
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj1
        HeapPage pgf = (HeapPage)this.cache.get(pid);
        if(pgf.isDirty() != null) {
        	Database.getCatalog().getDbFile(pid.getTableId()).writePage(pgf);
        }
        
        
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        //Collection<Page> cachePages = this.cache.values();
        
        for(Page page : this.cache.values()) {
        	if (page.isDirty() != null && tid.equals(page.isDirty())) {
        		this.flushPage(page.getId());
        	}
        	
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
        
        for (PageId pid: this.cache.keySet()) {
            Page p = this.cache.get(pid);
            //System.out.println(p+ " " + pid);
            
            try {
                this.flushPage(pid);
                this.discardPage(pid);
            } 
            catch (IOException e) {
            	System.out.println("IOException hit " + e.toString());
            }
            return;
        }
   	}
     
     /*
        HeapPage evictee = null;
        HeapPage temp;
        PageId evictPid = null;
        boolean firstGo = true;
        for (PageId id: this.cache.keySet()) {
        	if( firstGo == true) {
        		evictee = (HeapPage)this.cache.get(id);
        		firstGo = false;
        	}
            temp = (HeapPage)this.cache.get(id);
            if (temp.getTid() != null) {
		        if (evictee == null) {
		        	System.out.println("we are getting here though");
		            evictee = temp;
		        } else if (temp.getTid().getId() < evictee.getTid().getId()) {
		        	System.out.println(" we are getting here" + temp.getTid().getId());
		            evictee = temp;
		            evictPid = evictPid;
		        }
            }
        }
        
        try {
          	this.flushPage(evictPid);
        	this.discardPage(evictPid);
       	} 
        catch (IOException e) {
            System.out.println("IOException hit " + e.toString());
        }
        
        
  	}*/
        
   
   

}
