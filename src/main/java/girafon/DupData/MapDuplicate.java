package girafon.DupData;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 


// read a block of data, output n x blocks
public class MapDuplicate 
	extends Mapper<Object, Text, Text, Text>{
	
 
 
	private Random rand = new Random();
	private int dup = 1;
	List<List<Integer>> data = new  ArrayList<List<Integer>>();
	List<List<Integer>> dataDup;

	// for each items, we save the frequency.
	HashMap<Integer, Integer> items = new HashMap<Integer, Integer>();  
	
	 @Override
	protected void setup(Context context) throws IOException, InterruptedException {
		 dup = context.getConfiguration().getInt("duplication", 1);
	}
	  
	 public void map(Object key, Text value, Context context
	                 ) throws IOException, InterruptedException {
		 // add transaction to data 	 
		 String[] parts = value.toString().split("\\s+");
		 List<Integer> t = new ArrayList<Integer>();
		 for (String s : parts)
			 t.add(Integer.valueOf(s));
		
		 // update frequency of items in tranaction t
		 for (Integer x : t) {
			 if (items.containsKey(x)) {
				 Integer v = items.get(x) + 1;
				 items.put(x, v);
			 }
			 else
				 items.put(x, 1);
		 }
		 data.add(t);
	 }
	 
	 public void generateTransaction(Context context) throws IOException, InterruptedException {
		 dataDup = new  ArrayList<List<Integer>>();
		 for (int i = 0; i < data.size(); i++)
			 dataDup.add(new ArrayList<Integer>());
		 
		 // for each item x, we put it to  items.get(x) transactions 
		 for (Integer x : items.keySet()) {
			 for (int i = 0; i < items.get(x); i++) {
				 // try to put x to a random trasaction
				 
				 Integer t;
				 do {
					 t = rand.nextInt(data.size());
				 
				 } while (dataDup.get(t).contains(x));

				 // add items x to transaction t.
				 dataDup.get(t).add(x);
			 }
		 }
		 // now we will try to make sure that new data has the same length
		 //??
		 

		 
		 // output dataDup to HDFS
		 for (List<Integer> t : dataDup) {
			 StringBuilder s = new StringBuilder();
			 for (Integer x : t) 
				 s.append(x.toString() + " ");
			 // remove last space
			// s.deleteCharAt(s.length()-1);
			 context.write(new Text(""), new Text(s.toString()));
		 }
		 
	 }
	 
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 // we have all data, now we need to duplicate
		 for (int i = 0; i < dup; i++)
			 generateTransaction(context);
	 }	 
}
