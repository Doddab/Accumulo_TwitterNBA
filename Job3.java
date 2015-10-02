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

public class Job3 {
	public static class Map3 extends Mapper<Key,Value,Text,Text> {
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
	
	public static class Reduce3 extends Reducer<Text,Text,Text,Mutation> {
		TreeSet<String> trset = new TreeSet<String>();
		int ts = 20080906;
		HashMap<String,String> h = new HashMap<String,String>();
public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
	h.put("okcthunder", "Oklahoma City");
   	h.put("Nuggets", "Denver Nuggets");
   	h.put("TrailBlazers", "Portland Trailblazers");
   	h.put("UtahJazz", "Utah Jazz");
   	h.put("TWolves", "Minnesota Timberwolves");
   	h.put("Lakers", "LA Lakers");
   	h.put("Suns", "Phoenix Suns");
   	h.put("GSWarriors", "Golden State Warriors");
   	h.put("Clippers", "L.A. Clippers");
   	h.put("NBAKings", "Sacramento Kings");
   	h.put("GoSpursGo", "San Antonio Spurs");
   	h.put("Mavs", "Dallas Mavericks");
   	h.put("Hornets", "New Orleans Hornets");
   	h.put("Grizzlies", "Memphis Grizzlies");
   	h.put("Rockets", "Houston Rockets");
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
	    	Mutation mut = new Mutation(new Text("WESTLOSE"));
	    	String teamfullname = h.get(va[1]);
	    	String join = teamfullname+"   "+"#"+va[1]+"   "+cnt;
			mut.put(new Text("westlose"), new Text(tss), new Value(join.getBytes()));
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
		    	Mutation mut = new Mutation(new Text("WESTWIN"));
		    	String teamfullname = h.get(va[1]);
		    	String join = teamfullname+"   "+"#"+va[1]+"   "+cnt;
				mut.put(new Text("westwin"), new Text(tss), new Value(join.getBytes()));
				output.write(null, mut);
		  	}
		  trset.clear();
	  }
	  }
	}
}