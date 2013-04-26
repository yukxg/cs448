package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	
	private Iterator left;
	private Iterator right;
	private Predicate[] preds;
	private Tuple pt;
	private Tuple nt;

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
	  
	  this.left = left;
	  this.right = right;
	  this.preds = preds;
	  pt = null;
	  nt = null;
	  
	  this.schema = Schema.join(left.schema, right.schema);
	  
    //throw new UnsupportedOperationException("Not implemented");
	  //todo
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  
	  indent(depth);
	  String exp = "";
	  
	  for(int i = 0; i < preds.length; i++)
	  {
		  if(i == 0)
		  {
			  exp += preds[i].toString();
		  }
		  else
		  {
			  exp += "OR" + preds[i].toString();
		  }
	  }
	  
	  System.out.println(exp);
	  left.explain(depth + 1);
	  right.explain(depth + 1);
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    
	  left.restart();
	  right.restart();
	  
	  pt = null;
	  nt = null;
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    
	  return (left.isOpen() && right.isOpen());
	  
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  
	  left.close();
	  right.close();
	  
	  pt = null;
	  nt = null;
	  
	  //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  
	  if(!isOpen())
		  return false;
	  
	  while(left.hasNext())
	  {
		  
		  pt = left.getNext();
		  
//		  if(pt == null)
//		  {
//			  if(!left.hasNext())
//			  {
//				  nt = null;
//				  return false;
//			  }
//			  else
//			  {
//				  pt = left.getNext();
//			  }
//		  }
		  
		  while(right.hasNext())
		  {
			  Tuple rt = right.getNext();
			  Tuple candidate = Tuple.join(pt, rt, schema);
			  
			  boolean flag = true;
			  
			  for(Predicate pre : preds)
			  {
				  if(!pre.evaluate(candidate))
				  {
					  flag = false;
					  break;
				  }
			  }
			  
			  if(flag)
			  {
				  nt = candidate;
				  return true;
			  }
		  }
		  
		  right.restart();
	  }
	  
	  nt = null;
	  return false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  
	  if(nt == null)
	  {
		  throw new IllegalStateException("no more tuples");
	  }
	  
	  return nt;
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class SimpleJoin extends Iterator
