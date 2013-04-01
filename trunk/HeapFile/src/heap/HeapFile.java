package heap;

import java.util.*;

import chainexception.ChainException;
import global.GlobalConst;
import global.Page;
import global.PageId;
import global.RID;
import global.Minibase;

public class HeapFile {
	private String name;
	private ArrayList<PageId> pages;
	private int recordNumber;
	private HashSet<Integer> pids;//
	private boolean delete;
	private HFPage current;

	/**
	 * If the given name already denotes a file, this opens it; otherwise, this
	 * creates a new empty file. A null name produces a temporary heap file
	 * which requires no DB entry.
	 */

	public HeapFile(String name) throws ChainException {
		this.name = name;
		if (name == null) {
			// Temporary heap file
			PageId firstPageId;
			Page page = new Page();
			firstPageId = global.Minibase.BufferManager.newPage(page, 1);
			pages = new ArrayList<PageId>();
			pids = new HashSet<Integer>();
			pages.add(firstPageId);
			pids.add(firstPageId.pid);
			current = new HFPage(page);
			current.initDefaults();
			global.Minibase.BufferManager.unpinPage(firstPageId, true);
		} else {
			PageId firstPageId;
			Page page = new Page();
			firstPageId = global.Minibase.DiskManager.get_file_entry(name);
			if (firstPageId == null) {
				firstPageId = global.Minibase.BufferManager.newPage(page, 1);
				global.Minibase.DiskManager.add_file_entry(name, firstPageId);
				global.Minibase.BufferManager.unpinPage(firstPageId, true);
				pages = new ArrayList<PageId>();
				pids = new HashSet<Integer>();
				pages.add(firstPageId);
				pids.add(firstPageId.pid);
				global.Minibase.BufferManager.pinPage(firstPageId, page, false);
				current = new HFPage(page);
				current.initDefaults();
				global.Minibase.BufferManager.unpinPage(firstPageId, true);
				return;
			}
			pages = new ArrayList<PageId>();
			pids = new HashSet<Integer>();
			//add the new HFPages
			global.Minibase.BufferManager.pinPage(firstPageId, page, false);
			current.copyPage(page);
			pages.add(firstPageId);
			pids.add(firstPageId.pid);
			recordNumber+=amount(current);
			global.Minibase.BufferManager.unpinPage(firstPageId, false);
			PageId currentPageId=current.getNextPage();
			while(currentPageId.pid!=0&currentPageId.pid!=-1){
				HFPage temp=new HFPage();
				global.Minibase.BufferManager.pinPage(currentPageId, temp, false);
				pages.add(currentPageId);
				pids.add(currentPageId.pid);
				recordNumber+=amount(temp);
				global.Minibase.BufferManager.unpinPage(currentPageId, false);
				
			}
		}

	}
private int amount(HFPage hf){
	RID temp=hf.firstRecord();
	int amount=0;
	while(temp!=null){
		temp=hf.nextRecord(temp);
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
	public RID insertRecord(byte[] record) throws IllegalArgumentException {
		if (record.length > GlobalConst.MAX_TUPSIZE)
			throw new IllegalArgumentException("the record's size is too large");
		recordNumber++;
		for (int i = 0; i < pages.size(); i++) {
			PageId pid = pages.get(i);
			Page page = new Page();
			global.Minibase.BufferManager.pinPage(pid, page, false);
			HFPage hfpage = new HFPage();
			hfpage.setPage(page);
		}

		return null;

	}

	/**
	 * Reads a record from the file, given its id.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid is invalid
	 */
	public byte[] selectRecord(RID rid) {
		return null;

	}

	/**
	 * Updates the specified record in the heap file.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid or new record is invalid
	 */
	public void updateRecord(RID rid, byte[] newRecord) {

	}

	/**
	 * Deletes the specified record from the heap file.
	 * 
	 * @throws IllegalArgumentException
	 *             if the rid is invalid
	 */
	public boolean deleteRecord(RID rid) {
		return false;

	}

	/**
	 * Gets the number of records in the file.
	 */
	public int getRecCnt() {
		return 0;

	}

	/**
	 * Searches the directory for a data page with enough free space to store a
	 * record of the given size. If no suitable page is found, this creates a
	 * new data page.
	 */
	protected PageId getAvailPage(int reclen) {
		return null;

	}

	// ----auto fix methods-------

	public HeapScan openScan() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException {
		// TODO Auto-generated method stub
		return false;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		return null;
	}

}
