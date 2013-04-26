package externalSort;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

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
	private boolean cont = false;
	private boolean get_flag = true;
	
	private ArrayList<Tuple> left_iter;
	private ArrayList<Tuple> right_iter;
	private ArrayList<Tuple> final_result;
	private java.util.Iterator<Tuple> final_iter;
	//public static int tcount = 0;
	//public static int tcount2 = 0;
	//private int it;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SortMergeJoin(Iterator left, Iterator right, int i, int j) {

		left_iter = new ArrayList<Tuple>();
		right_iter = new ArrayList<Tuple>();
		final_result = new ArrayList<Tuple>();
		
		while(left.hasNext())
		{
			left_iter.add(left.getNext());
		}
		while(right.hasNext())
		{
			right_iter.add(right.getNext());
		}
		
		final int fi = i;
		final int fj = j;
		
		//sort
		switch(left.getSchema().fieldType(i)){
		
		case AttrType.INTEGER:
			Collections.sort(left_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					return o1.getIntFld(fi) - o2.getIntFld(fi);
				}
				
			});
			break;
			
		case AttrType.STRING:
			Collections.sort(left_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					if(o1.getStringFld(fi).compareTo(o2.getStringFld(fi)) < 0)
					{
						return -1;
					}
					else if(o1.getStringFld(fi).compareTo(o2.getStringFld(fi)) > 0)
					{
						return 1;
					}
					else
						return 0;
				}
				
			});
			break;
			
		case AttrType.FLOAT:
			Collections.sort(left_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					if(o1.getFloatFld(fi) < o2.getFloatFld(fi))
					{
						return -1;
					}
					else if(o1.getFloatFld(fi) > o2.getFloatFld(fi))
					{
						return 1;
					}
					else
						return 0;
				}
				
			});
			break;
		
		}
		
		switch(right.getSchema().fieldType(j)){
		
		case AttrType.INTEGER:
			Collections.sort(right_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					return o1.getIntFld(fj) - o2.getIntFld(fj);
				}
				
			});
			break;
			
		case AttrType.STRING:
			Collections.sort(right_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					if(o1.getStringFld(fj).compareTo(o2.getStringFld(fj)) < 0)
					{
						return -1;
					}
					else if(o1.getStringFld(fj).compareTo(o2.getStringFld(fj)) > 0)
					{
						return 1;
					}
					else
						return 0;
				}
				
			});
			break;
			
		case AttrType.FLOAT:
			Collections.sort(right_iter, new Comparator<Tuple>(){
				
				public int compare(Tuple o1, Tuple o2)
				{
					
					if(o1.getFloatFld(fj) < o2.getFloatFld(fj))
					{
						return -1;
					}
					else if(o1.getFloatFld(fj) > o2.getFloatFld(fj))
					{
						return 1;
					}
					else
						return 0;
				}
				
			});
			break;
		
		}

		//may need to restart left and right
		//it = i;
		//this.test_name = "SM_join";
		this.left = left;
		this.right = right;
		this.preds = new Predicate[] { new Predicate(AttrOperator.EQ,
				AttrType.FIELDNO, i, AttrType.FIELDNO, left.getSchema()
						.getCount() + j) };
		
		//pt = null;
		//nt = null;

		this.schema = Schema.join(left.getSchema(), right.getSchema());

		// throw new UnsupportedOperationException("Not implemented");
		
		ArrayList<Tuple> left_current = duplicateTuples(left_iter, i, 0);
		ArrayList<Tuple> right_current = duplicateTuples(right_iter, j, 1);
		
		while(!left_current.isEmpty() && !right_current.isEmpty())
		{
			
			switch(right.getSchema().fieldType(j))
			{
			case AttrType.INTEGER:
				if(left_current.get(0).getIntFld(i) == right_current.get(0).getIntFld(j))
				{
					for(Tuple a : left_current)
						for(Tuple b : right_current)
						{
							final_result.add(Tuple.join(a, b, schema));
						}
					left_current = duplicateTuples(left_iter, i, 0);
					right_current = duplicateTuples(right_iter, j, 1);
				}
				else if(left_current.get(0).getIntFld(i) < right_current.get(0).getIntFld(j))
				{
					left_current = duplicateTuples(left_iter, i, 0);
				}
				else
					right_current = duplicateTuples(right_iter, j, 1);
				break;
			
			case AttrType.FLOAT:
				if(left_current.get(0).getFloatFld(i) == right_current.get(0).getFloatFld(j))
				{
					for(Tuple a : left_current)
						for(Tuple b : right_current)
						{
							final_result.add(Tuple.join(a, b, schema));
						}
					left_current = duplicateTuples(left_iter, i, 0);
					right_current = duplicateTuples(right_iter, j, 1);
				}
				else if(left_current.get(0).getFloatFld(i) < right_current.get(0).getFloatFld(j))
				{
					left_current = duplicateTuples(left_iter, i, 0);
				}
				else
					right_current = duplicateTuples(right_iter, j, 1);
				break;
				
			case AttrType.STRING:
				if(left_current.get(0).getStringFld(i).equals(right_current.get(0).getStringFld(j)))
				{
					for(Tuple a : left_current)
						for(Tuple b : right_current)
						{
							final_result.add(Tuple.join(a, b, schema));
						}
					left_current = duplicateTuples(left_iter, i, 0);
					right_current = duplicateTuples(right_iter, j, 1);
				}
				else if(left_current.get(0).getStringFld(i).compareTo(right_current.get(0).getStringFld(j)) < 0)
				{
					left_current = duplicateTuples(left_iter, i, 0);
				}
				else
					right_current = duplicateTuples(right_iter, j, 1);
				break;
			
			}
			
		}
		
		final_iter = final_result.iterator();
		
	}
	
	private ArrayList<Tuple> duplicateTuples(ArrayList<Tuple> iter, int index, int lr)
	{
		//iter cannot be empty
		if(iter.isEmpty())
		{
			return new ArrayList<Tuple>();
		}
		
		int type;
		if(lr == 0)
		{
			type = left.getSchema().fieldType(index);
		}
		else
		{
			type = right.getSchema().fieldType(index);
		}
		
		ArrayList<Tuple> output = new ArrayList<Tuple>();
		switch(type)
		{
		case AttrType.INTEGER:
			int key = iter.get(0).getIntFld(index);
			while(!iter.isEmpty() && key == iter.get(0).getIntFld(index))
			{
				output.add(iter.remove(0));
			}
			break;
		
		case AttrType.FLOAT:
			float keyf = iter.get(0).getFloatFld(index);
			while(!iter.isEmpty() && keyf == iter.get(0).getFloatFld(index))
			{
				output.add(iter.remove(0));
			}
			break;
			
		case AttrType.STRING:
			String keys = iter.get(0).getStringFld(index);
			while(!iter.isEmpty() && keys.equals(iter.get(0).getStringFld(index)))
			{
				output.add(iter.remove(0));
			}
			break;
		
		}
		
		return output;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {

		indent(depth);
		String exp = "";

		for (int i = 0; i < preds.length; i++) {
			if (i == 0) {
				exp += preds[i].toString();
			} else {
				exp += "OR" + preds[i].toString();
			}
		}

		System.out.println(exp);
		left.explain(depth + 1);
		right.explain(depth + 1);

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {

		left.restart();
		right.restart();

		//pt = null;
		//nt = null;

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {

		return (left.isOpen() && right.isOpen());

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {

		left.close();
		right.close();

		//pt = null;
		//nt = null;

		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
//
//		if (!isOpen())
//			return false;
//
//		if(!get_flag)
//			return true;
//
//		while (left.hasNext() || cont) {
//
////			if(it == 2)
////				tcount++;
//			if (!cont)
//			{
//				pt = left.getNext();
////				if(it == 2)
////					tcount++;
//			}
//
//			// if(pt == null)
//			// {
//			// if(!left.hasNext())
//			// {
//			// nt = null;
//			// return false;
//			// }
//			// else
//			// {
//			// pt = left.getNext();
//			// }
//			// }
//
//			while (right.hasNext()) {
//				cont = true;
//				Tuple rt = right.getNext();
//				Tuple candidate = Tuple.join(pt, rt, schema);
//
//				boolean flag = true;
//
//				for (Predicate pre : preds) {
//					if (!pre.evaluate(candidate)) {
//						flag = false;
//						break;
//					}
//				}
//
//				if (flag) {
//					nt = candidate;
//					get_flag = false;
//					return true;
//				}
//			}
//
//			cont = false;
//			right.restart();
//			
//		}
//
//		nt = null;
//		return false;
		
		
		return final_iter.hasNext();
		
		// throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {

//		if (nt == null) {
//			throw new IllegalStateException("no more tuples");
//		}
//
////		if(it == 0)
////			tcount2++;
//		get_flag = true;
//		return nt;
		if(!final_iter.hasNext())
		{
			throw new IllegalStateException("no more tuples");
		}
		
		return final_iter.next();

		// throw new UnsupportedOperationException("Not implemented");
	}

} // public class SimpleJoin extends Iterator

