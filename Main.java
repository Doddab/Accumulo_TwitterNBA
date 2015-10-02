import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
	  private static Options opts;
	  private static Option passwordOpt;
	  private static Option usernameOpt;
	  private static String USAGE = "Main <instance name> <zoo keepers> <input dir> <output table>";

	  static {
	    usernameOpt = new Option("u", "username", true, "username");
	    passwordOpt = new Option("p", "password", true, "password");

	    opts = new Options();

	    opts.addOption(usernameOpt);
	    opts.addOption(passwordOpt);
	  }
	
	  public int run(String[] unprocessed_args) throws Exception {
		    Parser p = new BasicParser();

		    CommandLine cl = p.parse(opts, unprocessed_args);
		    String[] args = cl.getArgs();

		    String username = cl.getOptionValue(usernameOpt.getOpt(), "root");
		    String password = cl.getOptionValue(passwordOpt.getOpt(), "secret");

		    if (args.length != 4) {
		      System.out.println("ERROR: Wrong number of parameters: " + args.length + " instead of 4.");
		      return printUsage();
		    }
	  
		    ZooKeeperInstance instance = new ZooKeeperInstance(args[0], args[1]);
	        Connector conn = instance.getConnector(cl.getOptionValue(usernameOpt.getOpt(), "root"),cl.getOptionValue(passwordOpt.getOpt(), "secret")) ;
	        TableOperations tableOps = conn.tableOperations();
	        if (tableOps.exists(args[3])) {             
	            tableOps.delete(args[3]);
	        }
	        tableOps.create(args[3]);
	       
	
	SecurityOperationsImpl manager = new SecurityOperationsImpl(instance,new AuthInfo(username, stringtbyte(password), instance.getInstanceID()));
	manager.createUser("east", "east".getBytes(), new Authorizations("EAST"));
	manager.createUser("west", "west".getBytes(), new Authorizations("WEST"));
	manager.grantSystemPermission("east", SystemPermission.CREATE_TABLE);
	manager.grantSystemPermission("west", SystemPermission.CREATE_TABLE);
	manager.grantTablePermission("east", args[3], TablePermission.READ);
	manager.grantTablePermission("west", args[3], TablePermission.READ);
	manager.changeUserAuthorizations(username, new Authorizations("EAST", "WEST"));
	        
		  Job job1 = new Job(getConf(),Main.class.getName());
	      job1.setJarByClass(Job1.class);
	      job1.setInputFormatClass(TextInputFormat.class);
	      TextInputFormat.setInputPaths(job1, new Path(args[2]));
	      job1.setMapperClass(Job1.Map1.class);
	      job1.setMapOutputKeyClass(Text.class);
	      job1.setMapOutputValueClass(Text.class);
	      job1.setNumReduceTasks(1);
	      job1.setReducerClass(Job1.Reduce1.class);
	      job1.setOutputFormatClass(AccumuloOutputFormat.class);
	      job1.setOutputKeyClass(Text.class);
	      job1.setOutputValueClass(Mutation.class);
	      AccumuloOutputFormat.setOutputInfo(job1.getConfiguration(), "root", "acc".getBytes(), true, args[3]);
	      AccumuloOutputFormat.setZooKeeperInstance(job1.getConfiguration(),args[0],args[1]);
	      job1.waitForCompletion(true);
	      
	      Job job2 = new Job(getConf(),Main.class.getName());
	      job2.setJarByClass(Job2.class);
	      job2.setInputFormatClass(AccumuloInputFormat.class);
	      AccumuloInputFormat.setInputInfo(job2, "east", "east".getBytes(), args[3], new Authorizations("EAST"));
	      AccumuloInputFormat.setZooKeeperInstance(job2.getConfiguration(), args[0],args[1]);
	      job2.setMapperClass(Job2.Map2.class);
	      job2.setMapOutputKeyClass(Text.class);
	      job2.setMapOutputValueClass(Text.class);
	      job2.setNumReduceTasks(1);
	      job2.setReducerClass(Job2.Reduce2.class);
	      job2.setOutputFormatClass(AccumuloOutputFormat.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Mutation.class);
	      AccumuloOutputFormat.setOutputInfo(job2.getConfiguration(), "east", "east".getBytes(), true, "eastwinlose");
	      AccumuloOutputFormat.setZooKeeperInstance(job2.getConfiguration(),args[0],args[1]);
	      job2.waitForCompletion(true);
	      manager.grantTablePermission("east", "eastwinlose", TablePermission.READ);
	      //System.out.println("Enddddd 222222");
	      
	      Job job3 = new Job(getConf(),Main.class.getName());
	      job3.setJarByClass(Job3.class);
	      job3.setInputFormatClass(AccumuloInputFormat.class);
	      AccumuloInputFormat.setInputInfo(job3, "west", "west".getBytes(), args[3], new Authorizations("WEST"));
	      AccumuloInputFormat.setZooKeeperInstance(job3.getConfiguration(), args[0],args[1]);
	      job3.setMapperClass(Job3.Map3.class);
	      job3.setMapOutputKeyClass(Text.class);
	      job3.setMapOutputValueClass(Text.class);
	      job3.setNumReduceTasks(1);
	      job3.setReducerClass(Job3.Reduce3.class);
	      job3.setOutputFormatClass(AccumuloOutputFormat.class);
	      job3.setOutputKeyClass(Text.class);
	      job3.setOutputValueClass(Mutation.class);
	      AccumuloOutputFormat.setOutputInfo(job3.getConfiguration(), "west", "west".getBytes(), true, "westwinlose");
	      AccumuloOutputFormat.setZooKeeperInstance(job3.getConfiguration(),args[0],args[1]);
	      job3.waitForCompletion(true);
	      manager.grantTablePermission("west", "westwinlose", TablePermission.READ);
	      //System.out.println("Enddddd 3333");
	      return 1;
	 }
	  public static ByteBuffer stringtbyte(String msg){
          Charset charset = Charset.forName("UTF-8");
          CharsetEncoder encoder = charset.newEncoder();
          try{
              return encoder.encode(CharBuffer.wrap(msg));
          }catch(Exception e){e.printStackTrace();}
          return null;
      }
	  
	  private int printUsage() {
		    HelpFormatter hf = new HelpFormatter();
		    hf.printHelp(USAGE, opts);
		    return 0;
		  }

		  public static void main(String[] args) throws Exception {
		    int res = ToolRunner.run(CachedConfiguration.getInstance(), new Main(), args);
		    System.exit(res);
		  }
}