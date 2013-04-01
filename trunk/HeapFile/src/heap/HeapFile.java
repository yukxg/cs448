package heap;

import com.sun.tools.javac.util.Convert;

import global.Minibase;
import global.Page;
import global.PageId;
import global.RID;
import chainexception.ChainException;

public class HeapFile {
	
	PageId firstPageId;
	
	boolean fdelete;
	String filename;
	boolean tf;
	int tcount; //TODO

	/**
	   * If the given name already denotes a file, this opens it; otherwise, this
	   * creates a new empty file. A null name produces a temporary heap file which
	   * requires no DB entry.
	   */
	  public HeapFile(String name)
	  {
		  
		  //filename = null;
		  //TODO fdelete
		  
		  //if name == null TODO
		  if(name == null)
			  tf = true;
		  else
			  tf = false;
		  
		  filename = name;
		  Page page = new Page();
		  firstPageId = null;
		  if(name != null)
		  {
			  firstPageId = Minibase.DiskManager.get_file_entry(filename);
		  }
		  
		  if(firstPageId == null)
		  {
			  firstPageId = Minibase.BufferManager.newPage(page, 1);
			  //may need exception: no new page
			  
			  Minibase.DiskManager.add_file_entry(filename, firstPageId);
			  
			  HFPage firstHPage = new HFPage(page);
			  firstHPage.setCurPage(firstPageId);
			  
			  PageId end = new PageId(PageId.INVALID_PAGEID);
			  firstHPage.setNextPage(end);
			  firstHPage.setPrevPage(end);
			  Minibase.BufferManager.unpinPage(firstPageId, true);
			  
		  }
		  
		  fdelete = false;
		  
	  }
	 
	/**
	   * Deletes the heap file from the database, freeing all of its pages.
	   */
	  public void deleteFile() 
	  {
		  if(fdelete)
			  return; //no need to delete 
		  
		  fdelete = true;
		  
		  PageId current = new PageId();
		  current.pid = firstPageId.pid;
		  PageId next = new PageId();
		  next.pid = 0;
		  Page buffer = new Page();
		  HFPage currentH = new HFPage();
		  Tuple t;
		  
		  Minibase.BufferManager.pinPage(current, currentH, false);
		  
		  RID rid = new RID();
		  while(current.pid != PageId.INVALID_PAGEID)
		  {
			  for(rid = currentH.firstRecord(); rid != null; rid = currentH.nextRecord(rid))
			  {
				  //t = new Tuple(currentH.selectRecord(rid));
				  
				  byte[] temp = currentH.selectRecord(rid);
				  //offset = 0
				  
				  PageId tid = new PageId();
				  tid.pid = global.Convert.getIntValue(8, temp);
				  
				  Minibase.BufferManager.freePage(tid);
			  }
			  
			  next = currentH.getNextPage();
			  Minibase.BufferManager.freePage(current);
			  
			  current.pid = next.pid;
			  if(next.pid != PageId.INVALID_PAGEID)
			  {
				  Minibase.BufferManager.pinPage(current, currentH, false);
			  }
		  }
		  
		  Minibase.DiskManager.delete_file_entry(filename);
	  }
	 
	  /**
	   * Inserts a new record into the file and returns its RID.
	   *
	   * @throws IllegalArgumentException if the record is too large
	   */
	  public RID insertRecord(byte[] record) throws ChainException
	  {
		return null;
		  
	  }
	 
	  /**
	   * Reads a record from the file, given its id.
	   *
	   * @throws IllegalArgumentException if the rid is invalid
	   */
	  public byte[] selectRecord(RID rid)
	  {
		return null;
		  
	  }
	 
	/**
	   * Updates the specified record in the heap file.
	   *
	   * @throws IllegalArgumentException if the rid or new record is invalid
	   */
	  public void updateRecord(RID rid, byte[] newRecord)
	  {
		  
	  }
	 
	  /**
	   * Deletes the specified record from the heap file.
	   *
	   * @throws IllegalArgumentException if the rid is invalid
	   */
	  public boolean deleteRecord(RID rid)
	  {
		return false;
		  
	  }
	 
	/**
	   * Gets the number of records in the file.
	   */
	  public int getRecCnt()
	  {
		return 0;
		  
	  }
	 
	  /**
	   * Searches the directory for a data page with enough free space to store a
	   * record of the given size. If no suitable page is found, this creates a new
	   * data page.
	   */
	  protected PageId getAvailPage(int reclen)
	  {
		return null;
		  
	  }

	  //----auto fix methods-------
	  
	public HeapScan openScan() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException
	{
		// TODO Auto-generated method stub
		return false;
	}

	public Tuple getRecord(RID rid) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
