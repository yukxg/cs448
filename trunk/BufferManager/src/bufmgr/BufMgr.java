package bufmgr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import chainexception.ChainException;
import diskmgr.DiskMgr;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;

public class BufMgr {

	byte[][] bufPool = null;
	bufDescriptor[] bufDescr = null;
	String replacementPolicy;
	HashTable phash = null;
	PriorityQueue<Integer> readylist;
	int lookahead;
	int numbufs;
	
	/**
	 * Create the BufMgr object. Allocate pages (frames) for the buffer pool in
	 * main memory and make the buffer manage aware that the replacement policy
	 * is specified by replacerArg (i.e. LH, Clock, LRU, MRU etc.).
	 * 
	 * @param numbufs
	 *            number of buffers in the buffer pool
	 * @param prefetchSize
	 *            number of pages to be prefetched
	 * @param replacementPolicy
	 *            Name of the replacement policy
	 */
	public BufMgr(int numbufs, int prefetchSize, String replacementPolicy) {
		// initialize all the variables
		this.numbufs = numbufs;
		this.replacementPolicy = replacementPolicy;
		this.lookahead = prefetchSize;
		
		bufPool = new byte[numbufs][GlobalConst.PAGE_SIZE];
		bufDescr = new bufDescriptor[numbufs];
		phash = new HashTable(59);
		readylist = new PriorityQueue<Integer>();
		
		for (int i = 0; i < numbufs; i++)
			bufDescr[i] = new bufDescriptor();
		for(int i=0;i<numbufs;i++)
			readylist.add(prefetchSize);
			
		//System.out.println("number buffers is "+numbufs);
	};

	/**
	 * Pin a page. First check if this page is already in the buffer pool. If it
	 * is, increment the pin_count and return a pointer to this page. If the
	 * pin_count was 0 before the call, the page was a replacement candidate,
	 * but is no longer a candidate. If the page is not in the pool, choose a
	 * frame (from the set of replacement candidates) to hold this page, read
	 * the page (using the appropriate method from {\em diskmgr} package) and
	 * pin it. Also, must write out the old page in chosen frame if it is dirty
	 * before reading new page.__ (You can assume that emptyPage==false for this
	 * assignment.)
	 * 
	 * @param pageno
	 *            page number in the Minibase.
	 * @param page
	 *            the pointer point to the page.
	 * @param emptyPage
	 *            true (empty page); false (non-empty page)
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
			throws ChainException {
		if (emptyPage)
			return;
		int frame= phash.getframe(pageno.pid);
		//System.out.println("~~~"+frame);
		if (frame  != -1) {
			// page has already existed in the bufferpool
			if (bufDescr[frame].get_pin_count() == 0)
				readylist.remove(frame);
			bufDescr[frame].increase_pin_count();
			// page.setpage(bufPool[frame]);
		} else {
			// get the frame number from priority queue
			frame = readylist.remove();
			int pagenomber = pageno.pid;
			PageId temp = new PageId();
			temp.pid = pagenomber;
			//flush the old page
			if (bufDescr[frame].isDirty())
				flushPage(temp);
			// add the new pair to page hashtable

			System.out.println("in buffer manger "+pagenomber+" "+ frame);
			phash.addpage(pagenomber, frame);
			bufDescr[frame].setPage(pagenomber);
			bufDescr[frame].setPincount(1);
			bufDescr[frame].setdirty(false);
			//LRU LA policy
			for (int i = 0; i < lookahead; i++) {
				readylist.add(phash.getframe(pagenomber + i));
			}
			
		}
	};

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty==true if the client has modified the page. If so, this call should
	 * set the dirty bit for this frame. Further, if pin_count>0, this method
	 * should decrement it. If pin_count=0 before this call, throw an exception
	 * to report error. (For testing purposes, we ask you to throw an exception
	 * named PageUnpinnedException in case of error.)
	 * 
	 * @param pageno
	 *            page number in the Minibase.
	 * @param dirty
	 *            the dirty bit of the frame
	 */
	public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
		
		if(phash.getframe(pageno.pid) == -1)
		{
			new ChainException();
			return;
		}
		//TODO: Exception 
		
		if(bufDescr[phash.getframe(pageno.pid)].get_pin_count() == 0)
		{
			throw new PageUnpinnedException(null, "pin_count=0 before this call");
		}
		
		bufDescr[phash.getframe(pageno.pid)].setdirty(dirty);
		if(bufDescr[phash.getframe(pageno.pid)].get_pin_count() > 0)
		{
			bufDescr[phash.getframe(pageno.pid)].decrease_pin_count();
		}
		
		if(bufDescr[phash.getframe(pageno.pid)].get_pin_count() == 0)
		{
			readylist.add(phash.getframe(pageno.pid));
			//LRU LA policy
			//not a candidate before this call, however, after this call
			//the pin_count == 0, it is a candidate right now
		}

	};

	/**
	 * Allocate new pages. Call DB object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client of the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can't find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 * 
	 * @param firstpage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 * 
	 * @return the first page id of the new pages.__ null, if error.
	 */
	public PageId newPage(Page firstpage, int howmany) {

		//may need a db object at the top
		//DiskMgr DB = new DiskMgr();
		if(isBufferFull())
		{
			return null;
			//deallocate pages
		}
		
		
		PageId temp = new PageId();
		try {
			
			Minibase.DiskManager.allocate_page(temp, howmany);
			
			pinPage(temp, firstpage, false);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ChainException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return temp;
	};

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 * 
	 * @param globalPageId
	 *            the page number in the data base.
	 */
	public void freePage(PageId globalPageId) throws ChainException {
		//TODO Exception 
		
		//pin, =1, >1
		unpinPage(globalPageId, false);
		
		Minibase.DiskManager.deallocate_page(globalPageId);
		
		bufDescr[phash.getframe(globalPageId.pid)] = new bufDescriptor();
		phash.delete(globalPageId.pid);
		readylist.remove(phash.getframe(globalPageId.pid));
		
		
	};

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pageid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pageid) {
		int frame=phash.getframe(pageid.pid);
		if(frame!=-1){
			Page temp=new Page();
			temp.setpage(bufPool[frame]);
			//todo write page
			DiskMgr db=new DiskMgr();
			try {
				db.write_page(pageid, temp);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
			
	};

	/**
	 * Used to flush all dirty pages in the buffer poll to disk
	 * 
	 */
	public void flushAllPages() {
		// get the hash table
		ArrayList<node>[] table = phash.getAllPages();
		for (int i = 0; i < table.length; i++) {
			PageId temp = new PageId();
			for (int j = 0; j < table[i].size(); j++) {
				temp.pid = table[i].get(j).page;
				int tempframe = table[i].get(j).frame;
				//check the dirty page
				if (bufDescr[tempframe].get_dirtybit())
					flushPage(temp);
			}
		}

	};

	/**
	 * Gets the total number of buffer frames.
	 */
	public int getNumBuffers() {
		return numbufs;
	}

	/**
	 * Gets the total number of unpinned buffer frames.
	 */
	public int getNumUnpinned() {
		int temp = 0;
		for (int i = 0; i < bufDescr.length; i++)
			if (bufDescr[i].get_pin_count() == 0)
				temp++;
		return temp;
	}
	
	private boolean isBufferFull()
	{
		for(int i = 0; i < bufDescr.length; i++)
		{
			if(bufDescr[i].get_pin_count() == 0)
				return false;
		}
		
		return true;
	}

};