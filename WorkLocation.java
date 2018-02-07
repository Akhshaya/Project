import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


  public class WorkLocation {
		
  public static class Top5Mapper extends
		Mapper<LongWritable, Text, Text, Text> {
			public void map(LongWritable key, Text value, Context context
	        ) throws IOException, InterruptedException {
				try {
				String[] str = value.toString().split("\t");
				 if(str[4].equals("DATA ENGINEER") && str[1].equals("CERTIFIED"))
		            {
					 String abc= str[4]+"\t"+str[7];	            	
		             context.write(new Text(str[8]),new Text (abc));
		            }
		            
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
public static class YearPartitioner extends
   Partitioner < Text, Text >
   {
      public int getPartition(Text key, Text value, int numReduceTasks)
      {
         String[] str = value.toString().split("\t");
         int year = Integer.parseInt(str[1]);

         if(year==2011)
         {
            return 0;
         }
         else if(year==2012)
         {
            return 1;
         }
         else if(year==2013)
         {
            return 2;
         }
          else if(year==2014)
         {
            return 3;
         }
         else if(year==2015)
         {
            return 4;
         }
         else
         {
            return 5;
         }
}
      }
   
public static class Top5Reducer extends Reducer<Text, Text, NullWritable, Text>
{
      public TreeMap<Long, Text> tm = new TreeMap<Long, Text>();
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			long count=0;
			String a="";
			for(Text val:values)
			{
				String[] str = val.toString().split("\t");
				
					count++;
					a = str[1]+"\t"+key+"\t"+str[0];
				
				
			}
			String myValue = a+"\t"+count;
			tm.put(new Long(count), new Text(myValue));
			if(tm.size()>1)
			{
				tm.remove(tm.firstKey());
			}
	   }
		public void cleanup(Context con) throws IOException, InterruptedException
		{
			for(Text t:tm.descendingMap().values())
			{
				con.write(NullWritable.get(), t);
			}
		}
}


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //conf.set("name", "value")
    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
    Job job = Job.getInstance(conf, "Count");
    job.setJarByClass(WorkLocation.class);
    job.setMapperClass(Top5Mapper.class);
    job.setPartitionerClass(YearPartitioner.class);
    job.setReducerClass(Top5Reducer.class);
    job.setNumReduceTasks(6);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

