package bufmgr;

import global.PageId;

public class bufDescriptor {

	private int pin_count;
	private PageId page_number;
	private boolean dirtybit;
	
	public bufDescriptor()
	{
		pin_count = 0;
		page_number = new PageId();
		//how to get the unique pageid
		dirtybit = false;
	}
	
	//Accessors
	
	
	//Mutators
	
	//TODO
}
