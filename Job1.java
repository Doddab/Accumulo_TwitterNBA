import java.io.IOException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Job1{
	
  public static class Map1 extends Mapper<LongWritable,Text,Text,Text> {
    int x = 0;
     public void map(LongWritable key, Text value, Context output) throws IOException {
    	x += 1;
    	String[] words = value.toString().split(",");
    	String val = "";
    	for(int j=1;j<words.length;j++){
    		val = val+" "+words[j];
    	}
    	val = val+" "+x;
    	String fileName = ((FileSplit) output.getInputSplit()).getPath().getName();
    	String[] teamarr = fileName.split("\\.");
    	String teamname = teamarr[0];
        try {
            output.write(new Text(teamname), new Text(val));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
  
  public static class Reduce1 extends Reducer<Text,Text,Text,Mutation> {
		int ts = 20080906;
public void reduce(Text key, Iterable<Text> values, Context output) throws IOException, InterruptedException {
			int wincount = 0;
			int losecount = 0;
			for (Text va:values){
				String val = va.toString().toLowerCase();
				String[] sentence = val.split(" ");
					for(String word:sentence){
						if(word.equals("win") || word.equals("win.")){
							wincount += 1;
						} else if(word.equals("lose") || word.equals("lose.")){
							losecount += 1;
						} else {
							wincount += 0;
							losecount += 0;
						}
					}
			}
			String k = key.toString();
			String res = Integer.toString(wincount)+"!"+Integer.toString(losecount);
			ts += 1;
	    	String tss = Integer.toString(ts);
	    	if(k.equals("Celtics") || k.equals("Knicks") || k.equals("76ers") || k.equals("Nets") || 
	    			k.equals("Raptors")||k.equals("Bulls")||k.equals("Pacers")||k.equals("Bucks")||k.equals("Cavs")||
	    			k.equals("Pistons")||k.equals("MiamiHeat")||k.equals("Bobcats")||k.equals("Hawks")||
	    			k.equals("OrlandoMagic")||k.equals("Wizards")){
	    	ColumnVisibility privateVis = new ColumnVisibility("EAST");
			Mutation mu = new Mutation(new Text(k));
			mu.put(new Text("count"), new Text(tss),privateVis,new Value(res.getBytes()));
			output.write(null,mu);
	    	}
	    	else if(k.equals("okcthunder") || k.equals("Nuggets") || k.equals("TrailBlazers") || k.equals("UtahJazz") || 
	    			k.equals("TWolves")||k.equals("Lakers")||k.equals("Suns")||k.equals("GSWarriors")||k.equals("Clippers")||
	    			k.equals("NBAKings")||k.equals("GoSpursGo")||k.equals("Mavs")||k.equals("Hornets")||
	    			k.equals("Grizzlies")||k.equals("Rockets")){
	    	ColumnVisibility privateVis = new ColumnVisibility("WEST");
	    	Mutation mu = new Mutation(new Text(k));
			mu.put(new Text("count"), new Text(tss),privateVis,new Value(res.getBytes()));
			output.write(null,mu);	
	    	}
	    }
	}
  
}