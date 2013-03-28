package heap;

import chainexception.ChainException;
import global.RID;

public class HeapScan {

	/**

	* Constructs a file scan by pinning the directoy header page and initializing

	* iterator fields.

	*/

	protected HeapScan(HeapFile hf)
	{
		
	}


	protected void finalize() throws Throwable
	{
		
	}

	/**

	* Closes the file scan, releasing any pinned pages.

	*/
	public void close() throws ChainException  //TODO
	{
		
	}


	/**

	* Returns true if there are more records to scan, false otherwise.

	*/
	public boolean hasNext()
	{
		return false;
		
	}

	 

	/**

	* Gets the next record in the file scan.

	* @param rid output parameter that identifies the returned record

	* @throws IllegalStateException if the scan has no more elements

	*/
	public Tuple getNext(RID rid)
	{
		return null;
		
	}
	
}
