package girafon.DupData;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
 


// read a block of data, output n x blocks
public class MapDuplicate 
	extends Mapper<Object, Text, NullWritable, Text>{
	
 
 
	private Random rand = new Random();
	private int dup = 1;
	List<List<Integer>> data = new  ArrayList<List<Integer>>();
	List<List<Integer>> dataDup;
	private long count = 0; // total number of items in the data 
	
 
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
		 
		 count += t.size();
		 data.add(t);
		 
	 }
	 
	 public void generateTransaction(Context context) throws IOException, InterruptedException {
		 dataDup = new  ArrayList<List<Integer>>();
		 for (int i = 0; i < data.size(); i++)
			 dataDup.add(new ArrayList<Integer>( data.get(i)  ));
		 
		 long fail = 0;
		 
		
		 // swap count times 
		 for (long i = 0; i < count; i++) {
			 // random 2 transactions
			 int t1 = rand.nextInt(dataDup.size());   
			 int t2 = rand.nextInt(dataDup.size());
			 
			 // random 2 items 
			 int i1 = rand.nextInt(dataDup.get(t1).size());
			 int i2 = rand.nextInt(dataDup.get(t2).size());
			 
			 Integer itemI1 = dataDup.get(t1).get(i1);
			 Integer itemI2 = dataDup.get(t2).get(i2);
			 

			 // if i1 not in t2 and i2 not in t1
			 if (!dataDup.get(t1).contains(itemI2)) {
				 if (!dataDup.get(t2).contains(itemI1)) {
					 // remove i1 in t1 and i2 in t2
				
				
					 
					 // remove items at position i1 and i2
					 dataDup.get(t1).remove(i1);
					 dataDup.get(t2).remove(i2);
					 
					 // add items which was at position i1 and i2
					 dataDup.get(t1).add(itemI2);
					 dataDup.get(t2).add(itemI1);
 
				 }
		 	}
			else {
					fail ++;
//					 System.out.println("\n" + i1 + " " + i2 + " " + itemI1 + " " + itemI2);
//					 System.out.println(dataDup.get(t1));
//					 System.out.println(dataDup.get(t2));
				 }			 
		 }
		 
		 System.out.println("\n\n\n\n" + count + " " + fail );
		 
		 
		 
		 // output dataDup to HDFS
		 for (List<Integer> t : dataDup) {
			 StringBuilder s = new StringBuilder();
			 for (Integer x : t) 
				 s.append(x.toString() + " ");
			 // remove last space
			// s.deleteCharAt(s.length()-1);
			 context.write(NullWritable.get(), new Text(s.toString()));
		 }
		 
		 for (int i = 0; i < 10; i++)
			 System.out.println(dataDup.get(i));
	 }
	 
	 @Override
	 public void cleanup(Context context) throws IOException, InterruptedException {
		 // we have all data, now we need to duplicate
		 for (int i = 0; i < dup; i++)
			 generateTransaction(context);
	 }	 
}
