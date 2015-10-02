import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2 {
	public static class Map2 extends Mapper<Key,Value,Text,Text> {
		public void map(Key key, Value v, Context output) throws IOException {
			String k = key.toString();
			String k1 = k.substring(0, k.indexOf(" "));
			String[] va = v.toString().split("!");
			for (int i=0;i<2;i++){
				if(i==0){
					try{
						output.write(new Text("win"), new Text(va[0]+"!"+k1));
				}catch (InterruptedException e) {
					e.printStackTrace();
				}
				}else{
					try {
						output.write(new Text("lose"), new Text(va[1]+"!"+k1));
			          } catch (InterruptedException e) {
			            e.printStackTrace();
			          }
				}
		}
	}			
}
	
	public static class Reduce2 extends Reducer<Text,Text,Text,Mutation> {
		TreeSet<String> trset = new TreeSet<String>();
		int ts = 20080906;
		HashMap<String,String> h = new HashMap<String,String>();
public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
	h.put("Celtics", "Boston Celtics");
   	h.put("Knicks", "New York Knicks");
   	h.put("76ers", "Philadelphia 76ers" );
   	h.put("Nets", "New Jersey Nets");
   	h.put("Raptors", "Toronto Raptors");
   	h.put("Bulls", "Chicago Bulls"); 
   	h.put("Pacers", "Indiana Pacers");
   	h.put("Bucks", "Milwaukee Bucks");
   	h.put("Pistons", "Detroit Pistons");
   	h.put("Cavs", "Cleveland Cavaliers");
   	h.put("MiamiHeat", "Miami Heat"); 
   	h.put("OrlandoMagic", "Orlando Magic");
   	h.put("Hawks", "Atlanta Hawks");
   	h.put("Bobcats", "Charlotte Bobcats");
   	h.put("Wizards", "Washington Wizards");
	String val = "";
	for (Text value:values){
			val = value.toString();
			String[] v = val.split("!");
			String y = "0000".substring(v[0].length())+v[0];
			trset.add(y+"!"+v[1]);
		}
	  String k = key.toString();
	  Iterator<String> itr = trset.descendingIterator();
	  if(k.equals("lose")){
		  while(itr.hasNext()){
		  String[] va;
		  ts += 1;
	    	String tss = Integer.toString(ts);
	    	va = itr.next().toString().split("!");
	    	int cnt = Integer.parseInt(va[0]);
	    	Mutation mut = new Mutation(new Text("EASTLOSE"));
	    	String teamfullname = h.get(va[1]);
	    	String join = teamfullname+"   "+"#"+va[1]+"   "+cnt;
			mut.put(new Text("eastlose"), new Text(tss), new Value(join.getBytes()));
			output.write(null, mut);
	  	}
	  trset.clear();
	  }else{
		  while(itr.hasNext()){
			  String[] va;
			  ts += 1;
		    	String tss = Integer.toString(ts);
		    	va = itr.next().toString().split("!");
		    	int cnt = Integer.parseInt(va[0]);
		    	Mutation mut = new Mutation(new Text("EASTWIN"));
		    	String teamfullname = h.get(va[1]);
		    	String join = teamfullname+"   "+"#"+va[1]+"   "+cnt;
				mut.put(new Text("eastwin"), new Text(tss), new Value(join.getBytes()));
				output.write(null, mut);
		  	}
		  trset.clear();
	  }
	  
	  }
	}
}