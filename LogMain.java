package maxLog;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import maxLog.LogMapper;
import maxLog.LogReducer;

public class LogMain {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Maximum Log");
		job.setJarByClass(LogMain.class);
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000" + args[1]), conf);
			String fname = fs.listStatus(new Path(args[1]))[1].getPath().toString();
			FSDataInputStream in = null;
			in = fs.open(new Path(fname));
//            File Obj = new File("/home/hduser/Documents/Assignment2/part-r-00000");
            Scanner Reader = new Scanner(in);
            String key = "";
            int max = 0;
            
            while (Reader.hasNextLine()) {
                String data = Reader.nextLine();
                String[] vals = data.split("	");
//                System.out.println(Arrays.toString(vals));
                int count = Integer.parseInt(vals[1]);
                if (count > max) {
                	key = vals[0];
                	max = count;
                }
            }
            System.out.println("-----------------------------------------");
            System.out.println("IP address used maximum no of times:- ");
            System.out.println(key + " -> " + max);
            System.out.println("-----------------------------------------");
            Reader.close();
        }
        catch (FileNotFoundException e) {
            System.out.println("An error has occurred.");
            e.printStackTrace();
        }
	}
}
