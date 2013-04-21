package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	HashScan scan;
	HashIndex index;
	HeapFile file;
	SearchKey key;
	boolean openflag;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		this.index = index;
		this.schema = schema;
		this.key = key;
		this.file = file;
		scan = index.openScan(key);
		openflag = true;

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("in the explain: " + file.toString());
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// check the openflag status
		if (openflag) {
			openflag = false;
			scan.close();
		}
		// then we reopen the heapscan from heapscan
		openflag = true;
		scan = index.openScan(key);
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
		  if(openflag==false)
			  throw new IllegalStateException("getnext error");
		  try{
			  RID  rid=scan.getNext();
			  byte[] temp=file.selectRecord(rid);
			  Tuple tuple =new Tuple(schema,temp);
			  return tuple;
		  }
		  catch(Exception e){
			  throw new IllegalStateException("getnext error:fail to get tuple");
		  }
		//throw new UnsupportedOperationException("Not implemented");
	}

} // public class KeyScan extends Iterator
