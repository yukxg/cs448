package heap;

import java.util.*;

import chainexception.ChainException;
import global.*;

public class HeapFile {
	private String name;
	private HFPage current;
	private int recordNumber;
	private boolean delete;
	//pages to restore all the pageids
	//pids to restore all integer of pid
	private ArrayList<PageId> pages;
	private HashSet<Integer> pids;//

	/**
	 * If the given name already denotes a file, this opens it; otherwise, this
	 * creates a new empty file. A null name produces a temporary heap file
	 * which requires no DB entry.
	 */

	public HeapFile(String name) throws ChainException {
		this.name = name;
		PageId firstPageId;
		Page page = new Page();
		pages = new ArrayList<PageId>();
		pids = new HashSet<Integer>();
		if (name == null) {
			// Temporary heap file
			firstPageId = global.Minibase.BufferManager.newPage(page, 1);
			current = new HFPage(page);
			current.setCurPage(firstPageId);
			pages.add(firstPageId);
			pids.add(firstPageId.pid);
			global.Minibase.BufferManager.unpinPage(firstPageId, true);
		} else {

			firstPageId = global.Minibase.DiskManager.get_file_entry(name);
			if (firstPageId == null) {
				firstPageId = global.Minibase.BufferManager.newPage(page, 1);
				global.Minibase.DiskManager.add_file_entry(name, firstPageId);
				global.Minibase.BufferManager.unpinPage(firstPageId, true);
				pages.add(firstPageId);
				pids.add(firstPageId.pid);
				global.Minibase.BufferManager.pinPage(firstPageId, page, false);

				recordNumber = 0;
				current = new HFPage(page);
				current.setCurPage(firstPageId);
				global.Minibase.BufferManager.unpinPage(firstPageId, true);
				return;
			}
			// add the new HFPages
			global.Minibase.BufferManager.pinPage(firstPageId, page, false);
			//System.err.println("hahahaha"+page.getData());
			current=new HFPage(page);
			current.setData(page.getData());
			pages.add(firstPageId);
			pids.add(firstPageId.pid);
			recordNumber += amount(current);
			global.Minibase.BufferManager.unpinPage(firstPageId, false);
			PageId currentPageId = current.getNextPage();
			while (currentPageId.pid != 0 & currentPageId.pid != -1) {
				HFPage temp = new HFPage();
				global.Minibase.BufferManager.pinPage(currentPageId, temp,
						false);
				pages.add(currentPageId);
				pids.add(currentPageId.pid);
				recordNumber += amount(temp);
				global.Minibase.BufferManager.unpinPage(currentPageId, false);
				currentPageId = temp.getNextPage();
			}
		}

	}

	private int amount(HFPage hf) {
		RID temp = hf.firstRecord();
		int amount = 0;
		while (temp != null) {
			temp = hf.nextRecord(temp);
			amount++;
		}
		return amount;
	}

	/**
	 * Deletes the heap file from the database, freeing all of its pages.
	 * 
	 * @throws ChainException
	 */
	public void deleteFile() throws ChainException {
		if (delete)
			throw new ChainException(null, "files have already been deleted");

		for (int i = 0; i < pages.size(); i++)
			try {
				global.Minibase.DiskManager.deallocate_page(pages.get(i));
			} catch (Exception e) {
				throw new ChainException(null, "fail to deallocate page");
			}
		global.Minibase.DiskManager.delete_file_entry(name);
		pages = null;
		pids = null;
		recordNumber = 0;
		delete = true;
	}

	/**
	 * Inserts a new record into the file and returns its RID.
	 * 
	 * @throws IllegalArgumentException
	 *             if the record is too large
	 */
	public RID insertRecord(byte[] record) throws IllegalArgumentException,
			ChainException {
		if (record.length > GlobalConst.MAX_TUPSIZE)
			throw new IllegalArgumentException("the record's size is too large");
		
		for (int i = 0; i < pages.size(); i++) {
			PageId pid = pages.get(i);
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pid, page, false);
			HFPage hfpage = new HFPage(page);
			hfpage.setCurPage(pid);
			hfpage.setData(page.getData());
			if (hfpage.getFreeSpace() > record.length) {
				
				RID rid = hfpage.insertRecord(record);
				recordNumber++;
				global.Minibase.BufferManager.unpinPage(pid, true);
				return rid;
			}
			global.Minibase.BufferManager.unpinPage(pid, false);
		}

		Page page = new Page();
		PageId pid = global.Minibase.BufferManager.newPage(page, 1);
		HFPage hfpage = new HFPage(page);
		// initialize HFPage
		hfpage.initDefaults();
		hfpage.setCurPage(pid);
		//hfpage.print();
		RID rid = hfpage.insertRecord(record);
		pages.add(pid);
		pids.add(pid.pid);
		hfpage.setNextPage(pid);
		hfpage.setPrevPage(current.getCurPage());
		current = hfpage;
		global.Minibase.BufferManager.unpinPage(pid, true);
		//hfpage.print();
		//System.out.println ("The PID is "+pid);
		recordNumber++;
		return rid;
	}

	/**
	 * Reads a record from the file, given its id.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid is invalid
	 */
	public byte[] selectRecord(RID rid) {
		PageId pid = rid.pageno;
		if (!pids.contains(pid.pid))
			throw new IllegalArgumentException("the RID is invalid");
		Page page=new Page();
		global.Minibase.BufferManager.pinPage(pid, page, false);
		global.Minibase.BufferManager.unpinPage(pid, false);
		return page.getData();
	}

	/**
	 * Updates the specified record in the heap file.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid or new record is invalid
	 */
	public void updateRecord(RID rid, byte[] newRecord) {
		PageId pid = rid.pageno;
		if (!pids.contains(pid.pid))
			throw new IllegalArgumentException("the RID is invalid");
		Page page = new Page();
		global.Minibase.BufferManager.pinPage(pid, page, false);
		HFPage hfpage = new HFPage(page);
		if (newRecord.length != hfpage.selectRecord(rid).length) {
			throw new IllegalArgumentException("fail to update RID");
		}
		global.Minibase.BufferManager.unpinPage(rid.pageno, false);
	}

	/**
	 * Deletes the specified record from the heap file.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid is invalid
	 */
	public boolean deleteRecord(RID rid) {
		PageId pid = rid.pageno;
		if (!pids.contains(pid.pid))
			throw new IllegalArgumentException("the rid is invalid");
		try {
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pid, page, false);
			HFPage hfpage = new HFPage();
			hfpage.copyPage(page);
			hfpage.deleteRecord(rid);
			global.Minibase.BufferManager.unpinPage(pid, true);
			recordNumber--;
			return true;
		} catch (Exception e) {
			System.err.print("fail to delete record");
			return false;
		}

	}

	/**
	 * Gets the number of records in the file.
	 */
	public int getRecCnt() {
		return recordNumber;

	}

	/**
	 * Searches the directory for a data page with enough free space to store a
	 * record of the given size. If no suitable page is found, this creates a
	 * new data page.
	 */
	protected PageId getAvailPage(int reclen) {
		for (int i = 0; i < pages.size(); i++) {
			PageId pid = pages.get(i);
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pid, page, false);
			HFPage hfpage = new HFPage();
			hfpage.copyPage(page);
			if (hfpage.getFreeSpace() >= reclen) {
				global.Minibase.BufferManager.unpinPage(pid, true);
				return pid;
			}
			global.Minibase.BufferManager.unpinPage(pid, false);
		}

		Page page = new Page();
		PageId pid = global.Minibase.BufferManager.newPage(page, 1);
		HFPage hfpage = new HFPage(page);
		// initialize HFPage
		hfpage.setCurPage(pid);
		pages.add(pid);
		pids.add(pid.pid);
		current.setNextPage(pid);
		hfpage.setPrevPage(current.getCurPage());
		current = hfpage;
		global.Minibase.BufferManager.unpinPage(pid, true);
		return pid;
	}

	// ----auto fix methods-------

	public HeapScan openScan() {
		// TODO Auto-generated method stub
		return new HeapScan(this);
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException {
		// TODO Auto-generated method stub
		PageId pid = rid.pageno;
		if (!pids.contains(pid.pid))
			throw new IllegalArgumentException("the RID is invalid");
		Page page = new Page();
		global.Minibase.BufferManager.pinPage(pid, page, false);
		HFPage hfpage = new HFPage(page);
		if (newTuple.getLength() != hfpage.selectRecord(rid).length) {
			throw new IllegalArgumentException("fail to update RID");
		}
		global.Minibase.BufferManager.unpinPage(rid.pageno, false);
		return true;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		return null;
	}

	public Iterator<PageId> iterator() {
		return pages.iterator();
	}

}
