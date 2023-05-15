package maxLog;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable>{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String token = itr.nextToken();
			if (check(token)) {
				context.write(new Text(token), new IntWritable(1));
			}
//			context.write(new Text(token), new IntWritable(1));
		}
	}
	
	private boolean check(String token) {
	
		boolean dot = false;
		while (!token.isEmpty()) {
			char first = token.charAt(0);
			if (first == '.') {
				dot = true;
			}
			if (!Character.isDigit(first) && first != '.') {
				return false;
			}
			token = token.substring(1);
		}
		return dot;
	}
	
	
}
