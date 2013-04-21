package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	HeapFile file;
	BucketScan scan;
	HashIndex index;
	boolean openflag;

	public IndexScan(Schema schema, HashIndex index, HeapFile file) {
		this.schema = schema;
		this.index = index;
		scan = index.openScan();
		openflag = true;

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("in the explain indexscan: " + file.toString()
				+ " index: " + index.toString());
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		if (openflag) {
			openflag = false;
			scan.close();
		}
		// then we reopen the heapscan from heapscan
		openflag = true;
		scan = index.openScan();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return openflag;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		if (openflag) {
			scan.close();
			openflag = false;
		} else {
			System.out.println("there is no file need to be closed ");
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (openflag)
			return scan.hasNext();
		else
			return false;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if (openflag == false)
			throw new IllegalStateException("getnext error");
		try {
			RID rid = scan.getNext();
			byte[] temp = file.selectRecord(rid);
			Tuple tuple = new Tuple(schema, temp);
			return tuple;
		} catch (Exception e) {
			throw new IllegalStateException("getnext error:fail to get tuple");
		}
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the key of the last tuple returned.
	 */
	public SearchKey getLastKey() {
		return scan.getLastKey();
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns the hash value for the bucket containing the next tuple, or
	 * maximum number of buckets if none.
	 */
	public int getNextHash() {
		return scan.getNextHash();
		// throw new UnsupportedOperationException("Not implemented");
	}

} // public class IndexScan extends Iterator
