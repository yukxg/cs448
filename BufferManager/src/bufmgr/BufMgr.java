package bufmgr;

import java.util.PriorityQueue;

import chainexception.ChainException;
import global.*;

public class BufMgr {
	 
	byte[][]bufPool=null;
	bufDescriptor[] bufDescr=null;
	String replacementPolicy;
	HashTable phash=null;
	PriorityQueue<Integer> readylist;
/**
 * Create the BufMgr object.
 * Allocate pages (frames) for the buffer pool in main memory and
 * make the buffer manage aware that the replacement policy is
 * specified by replacerArg (i.e. LH, Clock, LRU, MRU etc.).
 *
 * @param numbufs number of buffers in the buffer pool
 * @param prefetchSize number of pages to be prefetched
 * @param replacementPolicy Name of the replacement policy
 */
public BufMgr(int numbufs, int prefetchSize, String replacementPolicy) {
	//initialize all the variables
	bufPool=new byte[numbufs][GlobalConst.PAGE_SIZE];
	this.replacementPolicy=replacementPolicy;
	bufDescr=new bufDescriptor[numbufs]; 
	for(int i=0;i<numbufs;i++)
		bufDescr[i]=new bufDescriptor();
	phash=new HashTable(59);
	readylist=new PriorityQueue<Integer>();
	//System.out.println(replacementPolicy);
};
 
/** 
* Pin a page.
* First check if this page is already in the buffer pool.
* If it is, increment the pin_count and return a pointer to this 
* page.
* If the pin_count was 0 before the call, the page was a 
* replacement candidate, but is no longer a candidate.
* If the page is not in the pool, choose a frame (from the 
* set of replacement candidates) to hold this page, read the 
* page (using the appropriate method from {\em diskmgr} package) and pin it.
* Also, must write out the old page in chosen frame if it is dirty 
* before reading new page.__ (You can assume that emptyPage==false for
* this assignment.)
*
* @param pageno page number in the Minibase.
* @param page the pointer point to the page.
* @param emptyPage true (empty page); false (non-empty page)
*/
 public void pinPage(PageId pageno, Page page, boolean emptyPage) throws ChainException {
	 if (emptyPage)
		 return;
	 int frame;
	 if((frame=phash.getframe(pageno.pid))!=-1){
		 //page has already existed in the bufferpool
		 if(bufDescr[frame].get_pin_count()==0)
			 readylist.remove(frame);
		 bufDescr[frame].increase_pin_count();
		// page.setpage(bufPool[frame]);
	 }
	 else
	 {
		 
	 }
 };
 
/**
* Unpin a page specified by a pageId.
* This method should be called with dirty==true if the client has
* modified the page.
* If so, this call should set the dirty bit 
* for this frame.
* Further, if pin_count>0, this method should 
* decrement it. 
*If pin_count=0 before this call, throw an exception
* to report error.
*(For testing purposes, we ask you to throw
* an exception named PageUnpinnedException in case of error.)
*
* @param pageno page number in the Minibase.
* @param dirty the dirty bit of the frame
*/
public void unpinPage(PageId pageno, boolean dirty) throws ChainException {
	int frame;
	if((frame=phash.getframe(pageno.pid))!=-1){
		//page have already existed in pool
		
	}
};
 
/** 
* Allocate new pages.
* Call DB object to allocate a run of new pages and 
* find a frame in the buffer pool for the first page
* and pin it. (This call allows a client of the Buffer Manager
* to allocate pages on disk.) If buffer is full, i.e., you 
* can't find a frame for the first page, ask DB to deallocate 
* all these pages, and return null.
*
* @param firstpage the address of the first page.
* @param howmany total number of allocated new pages.
*
* @return the first page id of the new pages.__ null, if error.
*/
public PageId newPage(Page firstpage, int howmany) {
	
	return null;
	};
 
/**
* This method should be called to delete a page that is on disk.
* This routine must call the method in diskmgr package to 
* deallocate the page. 
*
* @param globalPageId the page number in the data base.
*/
public void freePage(PageId globalPageId) throws ChainException {};
 
/**
* Used to flush a particular page of the buffer pool to disk.
* This method calls the write_page method of the diskmgr package.
*
* @param pageid the page number in the database.
*/
public void flushPage(PageId pageid) {};
 
/**
* Used to flush all dirty pages in the buffer poll to disk
*
*/
public void flushAllPages() {};
 
/**
* Gets the total number of buffer frames.
*/
public int getNumBuffers() {
	return 0;}
 
/**
* Gets the total number of unpinned buffer frames.
*/
public int getNumUnpinned() {
	return 0;}
 
};