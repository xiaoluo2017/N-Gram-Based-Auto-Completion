import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
  
		int threashold;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			threashold = conf.getInt("threashold", 20);
		}

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if((value == null) || (value.toString().trim()).length() == 0) {
				return;
			}
      
			String line = value.toString().trim();
			String[] wordsPlusCount = line.split("\t");
			
      if(wordsPlusCount.length < 2) {
				return;
			}
			
			String[] words = wordsPlusCount[0].split("\\s+");
			int count = Integer.valueOf(wordsPlusCount[1]);

			if (count < threashold) {
				return;
			}

			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < words.length - 1; i++) {
				sb.append(words[i]).append(" ");
			}

			Text outputKey = new Text(sb.toString().trim());
			Text outputValue = new Text(words[words.length - 1] + "=" + count);
			context.write(outputKey, outputValue);
		}
	}

	public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
  
		int n;

		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			n = conf.getInt("n", 5);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			TreeMap<Integer, List<String>> map = new TreeMap<Integer, List<String>>(Collections.<Integer>reverseOrder());
			for (Text value : values) {
				String[] currValue = value.toString().trim().split("=");
				String word = currValue[0];
				int count = Integer.valueOf(currValue[1]);
				if (map.containsKey(count)) {
					map.get(count).add(word);
				} else {
					List<String> list = new ArrayList<String>();
					list.add(word);
					map.put(count, list);
				}
			}
			Iterator<Integer> it = map.keySet().iterator();
			for (int j = 0; it.hasNext() && j < n; j++) {
				int currCount = it.next();
				for (String str : map.get(currCount)) {
					context.write(new DBOutputWritable(key.toString(), str, currCount), NullWritable.get());
					j++;
				}
			}
		}
	}
}
