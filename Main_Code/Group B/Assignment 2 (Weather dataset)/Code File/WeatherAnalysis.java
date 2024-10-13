import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.*;

public class WeatherAnalysis extends Configured implements Tool {
	final long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;
	
	public static class MapClass extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text values = new Text();
		public void map(LongWritable key, Text value,
						OutputCollector<Text, Text> output,
						Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer itr = new StringTokenizer(line);
			int counter = 0;
			String key_out = null;
			String value_str = null;
			boolean skip = false;
			loop:while (itr.hasMoreTokens() && counter<13) {
				String str = itr.nextToken();
				switch (counter) {
				case 0:
					key_out = str;
					if(str.contains("STN")){
						skip = true;
						break loop;
					}else{
						break;
					}
				case 2:
					int hour = Integer.valueOf(str.substring(str.lastIndexOf("_")+1, str.length()));
					str = str.substring(4,str.lastIndexOf("_")-2);	
					if(hour>4 && hour<=10){
						str = str.concat("_section1");
					}else if(hour>10 && hour<=16){
						str = str.concat("_section2");
					}else if(hour>16 && hour<=22){
						str = str.concat("_section3");
					} else{ str = str.concat("_section4");
					}
					
					key_out = key_out.concat("_").concat(str);
					break;
				case 3:
					if(str.equals("9999.9")){
						skip = true;
						break loop;
					}else{
						value_str = str.concat(" ");
						break;
					}
				case 4:
					if(str.equals("9999.9")){
						skip = true;
						break loop;
					}else{
						value_str = value_str.concat(str).concat(" ");
						break;
					}
				case 12:
					if(str.equals("999.9")){
						skip = true;
						break loop;
					}else{ value_str = value_str.concat(str).concat(" ");
						break;
					}
				default: break;
				}
				counter++;
			}
			if(!skip){
				word.set(key_out);
				values.set(value_str);
				output.collect(word, values);
			}
		}
	}

	public static class Reduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		private Text value_out_text = new Text();
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double sum_temp = 0;
			double sum_dew = 0;
			double sum_wind = 0;
			int count = 0;
			
			while (values.hasNext()) {
				String str = values.next().toString();
				StringTokenizer itr = new StringTokenizer(str);
				int count_vector = 0;
				while (itr.hasMoreTokens()) {
					String nextToken = itr.nextToken(" ");
					if(count_vector==0){
						sum_temp += Double.valueOf(nextToken);
					}
					if(count_vector==1){
						sum_dew += Double.valueOf(nextToken);
					}
					if(count_vector==2){
						sum_wind += Double.valueOf(nextToken);
					}
					count_vector++;
				}
				count++;
			}
			double avg_tmp = sum_temp / count;
			double avg_dew = sum_dew / count;
			double avg_wind = sum_wind / count;
			System.out.println(key.toString()+" count is "+count+" sum of temp is "+sum_temp+" sum of dew is "+sum_dew+" sum of wind is "+sum_wind+"\n");
			String value_out = String.valueOf(avg_tmp).concat(" ").concat(String.valueOf(avg_dew)).concat(" ").concat(String.valueOf(avg_wind));
			value_out_text.set(value_out);
			output.collect(key, value_out_text);
		}
	}
	static int printUsage() {
		System.out.println("weather [-m <maps>] [-r <reduces>] <job_1 input> <job_1 output> <job_2 output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
	
	public int run(String[] args) throws Exception {
		Configuration config = getConf();
		
		JobConf conf = new JobConf(config, WeatherAnalysis.class);
		conf.setJobName("Weather Job1");

		
		conf.setOutputKeyClass(Text.class);
		
		conf.setOutputValueClass(Text.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setMapperClass(MapClass.class);

		conf.setReducerClass(Reduce.class);
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < args.length; ++i) {
			try {
				if ("-m".equals(args[i])) {
					conf.setNumMapTasks(Integer.parseInt(args[++i]));
				} else if ("-r".equals(args[i])) {
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				} else {
					other_args.add(args[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + args[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
					args[i-1]);
				return printUsage();
			}
		}	

		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));
		JobClient.runJob(conf);
		return 0;
	}
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WeatherAnalysis(), args);
		System.exit(res);
	}
}

