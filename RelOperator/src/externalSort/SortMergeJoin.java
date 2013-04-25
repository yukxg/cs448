package externalSort;

import global.AttrOperator;
import global.AttrType;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Schema;
import relop.SimpleJoin;
import relop.Tuple;

//public class SortMergeJoin extends Iterator {
//	
//	//private Iterator left;
//	//private Iterator right;
//	//private int col1;
//	//private int col2;
//	private Predicate[] preds;
//	private SimpleJoin join;
//	//private Tuple pt;
//	//private Tuple nt; 
//
//	public SortMergeJoin(Iterator left, Iterator right, int i, int j)
//	{
//		//this.left = left;
//		//this.right = right;
//		//pt = null;
//		//nt = null;
//		
//		
//		preds = new Predicate[] { new Predicate(AttrOperator.EQ,
//		          AttrType.FIELDNO, i, AttrType.FIELDNO, j) };
//		  
////		if(left == null)
////			System.err.println("left");
////		if(right == null)
////			System.err.println("right");
////		if(preds == null)
////			System.err.println("preds");
//		join = new SimpleJoin(left, right, preds);
//		this.schema = Schema.join(left.getSchema(), right.getSchema());
//		
//	}
//
//	@Override
//	public void explain(int depth) {
//		// TODO Auto-generated method stub
//		join.explain(depth);
//		
//	}
//
//	@Override
//	public void restart() {
//		// TODO Auto-generated method stub
//		//left.restart();
//		//right.restart();
//
//		//pt = null;
//		//nt = null;
//		join.restart();
//
//	}
//
//	@Override
//	public boolean isOpen() {
//		// TODO Auto-generated method stub
//		//return (left.isOpen() && right.isOpen());
//		return join.isOpen();
//	}
//
//	@Override
//	public void close() {
//		// TODO Auto-generated method stub
//		//left.close();
//		//right.close();
//
//		//pt = null;
//		//nt = null;
//		
//		join.close();
//		
//		//TODO col1 & col2
//		
//	}
//
//	@Override
//	public boolean hasNext() {
//		// TODO Auto-generated method stub
//		//return false;
//		
//		return join.hasNext();
//	}
//
//	@Override
//	public Tuple getNext() {
//		// TODO Auto-generated method stub
//		//return null;
//		
//		return join.getNext();
//	}
//	
//	@Override
//	public int execute()
//	{
//		return join.execute();
//	}
//
//}


public class SortMergeJoin extends Iterator {
	
	private Iterator left;
	private Iterator right;
	private Predicate[] preds;
	private Tuple pt;
	private Tuple nt;

  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SortMergeJoin(Iterator left, Iterator right, int i, int j) {
	  
	  if(left.getSchema() == null)
		  System.err.println("left_join");
	  if(right.getSchema() == null)
		  System.err.println("right_join");
	  
	  this.test_name = "SM_join";
	  this.left = left;
	  this.right = right;
	  this.preds = new Predicate[] { new Predicate(AttrOperator.EQ, AttrType.FIELDNO, i, AttrType.FIELDNO, left.getSchema().getCount()+j) };
	  pt = null;
	  nt = null;
	  
	  
	  this.schema = Schema.join(left.getSchema(), right.getSchema());
	  
    //throw new UnsupportedOperationException("Not implemented");
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

