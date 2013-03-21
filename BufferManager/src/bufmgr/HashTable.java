package bufmgr;

import java.util.ArrayList;

class node {
	int page;
	int frame;
	public node(int page, int frame) {
		page = this.page;
		frame = this.frame;
	}
}

public class HashTable {
	ArrayList<node>[] table;
	int size;

	public HashTable(int size) {
		size = this.size;
		table = new ArrayList[size];
	}

	public void addpage(int page,int frame){
		node temp=new node(page,frame);
		ArrayList index=table[(20*page+10)%size];
		index.add(temp);
	}
	public int getframe(int page){
		ArrayList index=table[(20*page+10)%size];
		node temp;
		for(int i=0;i<index.size();i++){
			temp=(node) index.get(i);
			if(temp.page==page)
				return temp.frame;
		}
		return -1;
	}
	public void delete(int page){
		ArrayList index=table[(20*page+10)%size];
		node temp;
		for(int i=0;i<index.size();i++){
			temp=(node) index.get(i);
			if(temp.page==page)
				index.remove(i);
		}
		
	}
}
