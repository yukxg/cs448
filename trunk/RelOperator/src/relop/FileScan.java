package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  /**
   * Constructs a file scan, given the schema and heap file.
   */
	HeapScan scan;
	HeapFile file;
	RID rid;
	boolean openflag;
	
  public FileScan(Schema schema, HeapFile file) {
	  this.schema=schema;
	  this.file=file;
	  scan=file.openScan();
	  rid=new RID();
	  openflag=true;
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
	  indent(depth);
	  System.out.println("in the explain: "+file.toString());
   // throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  //check the openflag status
	  if(openflag){
		  openflag=false;
		  scan.close();
	  }
	  //then we reopen the heapscan from heapscan
	  openflag=true;
	  scan=file.openScan();
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return openflag;
  //  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	//if we opened a heapscan we close it, otherwise, we do nothing
	  if(openflag){
		  scan.close();
		  openflag=false;
	  }
	  else{
		  System.out.println("there is no file need to be closed ");
	  }
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(openflag)
		  return scan.hasNext();
	  else
		  return false;
	  
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if(openflag==false)
		  throw new IllegalStateException("getnext error");
	  try{
		  byte[] temp=scan.getNext(rid);
		  Tuple tuple =new Tuple(schema,temp);
		  return tuple;
	  }
	  catch(Exception e){
		  throw new IllegalStateException("getnext error:fail to get tuple");
	  }
  //  throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  RID temp=new RID();
	  temp.copyRID(rid);
	  return temp;
   // throw new UnsupportedOperationException("Not implemented");
  }

} // public class FileScan extends Iterator
