package externalSort;

import java.util.Comparator;

import relop.Tuple;

public class TupleCompare implements Comparator<Tuple>{
	
	public int compare(Tuple o1, Tuple o2)
	{
		
		return ((Integer)o1.getField(0)).compareTo((Integer)o2.getField(0));
	}

}
