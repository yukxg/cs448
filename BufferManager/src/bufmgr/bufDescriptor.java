package bufmgr;

import global.PageId;

public class bufDescriptor {

	private int pin_count;
	private PageId page_number;
	private boolean dirtybit;

	public bufDescriptor() {
		pin_count = 0;
		page_number = new PageId();
		// how to get the unique pageid
		dirtybit = false;
	}

	// Accessors
	public int get_pin_count() {
		return pin_count;
	}

	public PageId get_page_number() {
		return page_number;
	}

	public boolean get_dirtybit() {
		return dirtybit;
	}

	public boolean isDirty() {
		return dirtybit;
	}

	// Mutators
	public void increase_pin_count() {
		pin_count++;
	}

	public void decrease_pin_count() {
		pin_count--;
	}

	public void setPage(int pageid) {
		page_number.pid = pageid;
	}

	public void setPincount(int pin) {
		pin_count = pin;
	}

	public void setdirty(boolean dirty) {
		dirtybit = dirty;
	}
	// TODO
}
