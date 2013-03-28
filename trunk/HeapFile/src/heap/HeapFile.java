package heap;

import chainexception.ChainException;
import global.PageId;
import global.RID;

public class HeapFile {

	/**
	   * If the given name already denotes a file, this opens it; otherwise, this
	   * creates a new empty file. A null name produces a temporary heap file which
	   * requires no DB entry.
	   */
	  public HeapFile(String name)
	  {
		  
	  }
	 
	/**
	   * Deletes the heap file from the database, freeing all of its pages.
	   */
	  public void deleteFile() 
	  {
		  
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
