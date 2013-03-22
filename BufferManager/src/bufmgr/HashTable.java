package bufmgr;

import java.util.ArrayList;

class node {
	int page;
	int frame;

	public node(int page, int frame) {
		this.page = page;
		this.frame = frame;
	}
}

public class HashTable {
	ArrayList<node>[] table;
	int size;

	public HashTable(int size) {
		this.size = size;
		table = new ArrayList[size];
		for (int i = 0; i < table.length; i++)
			table[i] = new ArrayList<node>();
	}

	public void addpage(int page, int frame) {
		node temp = new node(page, frame);
		ArrayList index = table[(20 * page + 10) % size];
		if(getframe(page)!=-1)
			return;
		index.add(temp);
	}

	public int getframe(int page) {
		ArrayList index = table[(20 * page + 10) % size];
		node temp;
		// System.out.println("in hash table"+index.size());
		if (index == null)
			return -1;
		for (int i = 0; i < index.size(); i++) {
			temp = (node) index.get(i);
			if (temp.page == page)
				return temp.frame;
		}
		return -1;
	}

	public void delete(int page) {
		// System.out.println("in hash table"+page);
		// System.out.println("in hash table "+(20 * page + 10) % size);
		ArrayList index = table[(20 * page + 10) % size];
		node temp;
		for (int i = 0; i < index.size(); i++) {
			temp = (node) index.get(i);
			if (temp.page == page)
				index.remove(i);
		}
	}

	public ArrayList<node>[] getAllPages() {
		return table;
	}
}
