package bufmgr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import chainexception.*;
import diskmgr.DiskMgr;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.*;

public class BufMgr {

	byte[][] bufPool = null;
	bufDescriptor[] bufDescr = null;
	String replacementPolicy;
	HashTable phash = null;
	LinkedList<Integer> readylist;
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
	public BufMgr(int numbufs, int prefetchSize, String replacementPolicy)
			throws ChainException {
		// initialize all the variables
		this.numbufs = numbufs;
		this.replacementPolicy = replacementPolicy;
		this.lookahead = prefetchSize;

		bufPool = new byte[numbufs][GlobalConst.PAGE_SIZE];
		for (int i = 0; i < numbufs; i++)
			for (int j = 0; j < GlobalConst.PAGE_SIZE; j++)
				bufPool[i][j] = (byte) 0;
		bufDescr = new bufDescriptor[numbufs];
		phash = new HashTable(29);
		readylist = new LinkedList<Integer>();

		for (int i = 0; i < numbufs; i++)
			bufDescr[i] = new bufDescriptor();
		for (int i = 0; i < numbufs; i++)
			readylist.addLast(i);

		// System.out.println("number buffers is "+numbufs);
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
		int frame = phash.getframe(pageno.pid);
		// System.out.println("~~~"+frame);
		if (frame != -1) {
			// page has already existed in the bufferpool
			if (bufDescr[frame].get_pin_count() == 0)
				readylist.remove(new Integer(frame));
			bufDescr[frame].increase_pin_count();
			// bufPool[frame] = page.getpage();
			page.setpage(bufPool[frame]);
		} else {
			// get the frame number from priority queue
			if (readylist.size()-1 == 0)
				throw new BufferPoolExceededException(null,
						"used up all the readylist");
			frame = readylist.pollFirst();
			// System.out.println("replace~~ " + frame+" "+pageno.pid );
			// Iterator<Integer> lt=readylist.iterator();
			// while(lt.hasNext())
			// System.out.print(" "+lt.next());
			// System.out.println();

			// System.out.println("pinpage~~ " + frame + "  |  "
			// + readylist.size());
			// int pagenomber = pageno.pid;
			int pagenomber = bufDescr[frame].get_page_number().pid;
			// System.out.println("Main: pinPage pid is " + pagenomber);
			if (pagenomber != -1) {
				PageId tempid = new PageId();
				tempid.pid = pagenomber;
				// flush the old page

				if (bufDescr[frame].isDirty())
					flushPage(tempid);
				// add the new pair to page hashtable
				// bufPool[frame] = page.getpage();
				// System.out.println("~~~" + page.getpage());
				// System.out.println("~~~!!" + bufPool[frame]);

				phash.delete(pagenomber);
			}
			phash.addpage(pageno.pid, frame);
			page.setpage(bufPool[frame]);
			try {
				Minibase.DiskManager.read_page(pageno, page);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				throw new ChainException(e, "Unable to read page.");
			}
			for (int i = 0; i < page.getpage().length; i++) {
				bufPool[frame][i] = page.getpage()[i];
				// System.out.print(temp.getpage()[i]);
			}
			// System.out.println(temp.getData());
			bufDescr[frame].setPage(pagenomber);
			bufDescr[frame].setPincount(1);
			bufDescr[frame].setdirty(false);
			// LRU-A policy
			for (int i = 1; i <= numbufs; i++) {
				int index = phash.getframe(pageno.pid + i);
				if (!readylist.contains(index) && index != -1)
					readylist.add(index);
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
	 * @throws
	 */
	public void unpinPage(PageId pageno, boolean dirty)
			throws PagePinnedException {

		if (phash.getframe(pageno.pid) == -1) {
			//throw new HashEntryNotFoundException(null,
				//	"PageId is not found in the buffer pool");

			return;
		} else {
			// TODO: Exception

			// if (bufDescr[phash.getframe(pageno.pid)].get_pin_count() == 0) {
			// throw new PageUnpinnedException(null,
			// "pin_count=0 before this call");
			// }
			if (bufDescr[phash.getframe(pageno.pid)].get_pin_count() == 0)
				throw new PagePinnedException(null,
						"pin_count=0 before this call");
			else {

				bufDescr[phash.getframe(pageno.pid)].setdirty(dirty);
				bufDescr[phash.getframe(pageno.pid)].decrease_pin_count();

				if (bufDescr[phash.getframe(pageno.pid)].get_pin_count() == 0) {

					if (!readylist.contains(phash.getframe(pageno.pid)))
						readylist.addLast(phash.getframe(pageno.pid) % numbufs);
					// System.out.println("*** "+phash.getframe(pageno.pid));
				}
				// LRU LA policy
				// not a candidate before this call, however, after this call
				// the pin_count == 0, it is a candidate right now
			}
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
	public PageId newPage(Page firstpage, int howmany) throws ChainException {

		// may need a db object at the top
		// DiskMgr DB = new DiskMgr();
		if (isBufferFull()) {
			throw new ChainException(null, "fail to new a page");
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
		// TODO Exception
		int frame = phash.getframe(globalPageId.pid);
		if (frame != -1)
			if (bufDescr[frame].get_pin_count() > 1)
				throw new PagePinnedException(null, "page is pinned");

			else {
				try {
					if (bufDescr[frame].get_pin_count() == 1)
						unpinPage(globalPageId, false);

					try {
						Minibase.DiskManager.deallocate_page(globalPageId);
					} catch (ChainException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					bufDescr[phash.getframe(globalPageId.pid)] = new bufDescriptor();
					phash.delete(globalPageId.pid);
					readylist.remove(new Integer(phash
							.getframe(globalPageId.pid)));
				} catch (Exception e) {
					throw new ChainException(null, "error in the free page ");
				}
			}
	};

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pageid
	 *            the page number in the database.
	 */
	public void flushPage(PageId pageid) {
		int frame = phash.getframe(pageid.pid);
		if (frame != -1) {
			Page temp = new Page();
			temp.setpage(bufPool[frame]);
			// todo write page
			try {
				Minibase.DiskManager.write_page(pageid, temp);
			} catch (InvalidPageNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileIOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
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
				// check the dirty page
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
		int temp = -1;
		for (int i = 0; i < bufDescr.length; i++)
			if (bufDescr[i].get_pin_count() == 0)
				temp++;
		return temp;
	}

	private boolean isBufferFull() {
		for (int i = 0; i < bufDescr.length; i++) {
			if (bufDescr[i].get_pin_count() == 0)
				return false;
		}

		return true;
	}

	public void check()
	{
		System.out.println(readylist.size());
	}
};