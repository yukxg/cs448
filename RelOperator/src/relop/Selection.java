package relop;

import externalSort.SortMergeJoin;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {
	Iterator iterator;
	Predicate[] predicate;
	Tuple tuple;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		
		if(iter.schema == null)
			System.err.println("sel");
		
		this.test_name = "select";
		this.schema = iter.schema;
		this.iterator = iter;
		this.predicate = preds;
		tuple = null;
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
			indent(depth);
			System.out.println("Selection: "+predicate.toString());
			depth++;
			iterator.explain(depth);
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		iterator.restart();
		tuple=null;
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		return iterator.isOpen();
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		iterator.close();
		tuple=null;
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		
		if (!isOpen())
			return false;
		else{
			while(iterator.hasNext()){
				//boolean temp=false;
				boolean passflag=true;
				Tuple tp=iterator.getNext();
				for(Predicate pre:predicate){
					if(!pre.evaluate(tp)){
						passflag=false;
						break;
					}
					
				}
				
				if(passflag){
					tuple=tp;
					return true;
				}
				
			}return false;
			
		}
		//throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		if(tuple==null)
			throw new IllegalStateException("getnext error");
		else
			return tuple;
		//throw new UnsupportedOperationException("Not implemented");
	}

} // public class Selection extends Iterator
