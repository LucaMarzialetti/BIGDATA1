package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;


public class TotalPerMonth extends Configured implements Tool{
	
	/*public static enum COUNTER {
		  TotalCounter;
		}*/
	public static void main (String[] args) throws Exception{
		
		int res=ToolRunner.run(new Configuration(),  new TotalPerMonth(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TotalPerMonth <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf);
		job.setJobName("TotalPerMonth");
		job.setJarByClass(TotalPerMonth.class);
		job.setMapperClass(TotalPerMonthMapper.class);
		job.setReducerClass(TotalPerMonthReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		return job.waitForCompletion(true) ? 0 : -1;
	}

	/**
	 * Dato un formato in input AAAA/MM/DD,(ITEM_UNIT_COST ITEM)*N con N a piacere, si mappa emettendo coppie <ITEM_UNIT_COST ITEM, MM-AAAA>
	 */
	public static class TotalPerMonthMapper extends Mapper<Object, Text, Text, Text> {

		private Text year_and_month_date = new Text();
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(",");
			String date = parts[0];
			StringTokenizer st=new StringTokenizer(date,"-");
			/*data*/
			String anno = st.nextToken();
			String mese = st.nextToken();
			String res=	mese+"-"+anno;	
			Text year_and_month = new Text(res);
			
			/*items*/
			int cont=1;
			year_and_month_date.set(year_and_month);
			while (cont<parts.length) {
				context.write(new Text(parts[cont]),year_and_month_date);
				cont=cont+1;
			}
		}
	}

	/**
	 * input dallo shuffle/sort= <ITEM_UNIT_COST ITEM, [lista di MM-AAAA in cui l'item compare]>
	 * nel reducer, si estrae nome e costo dell'item, e si crea una mappa <MM-AAAA,RICAVATO_MM_TOTALE>, ordinata per mese, scrivendo poi in output
	 * coppie <"NOME_ITEM","AAAA-MM:RICAVATO_MM_TOTALE">
	 */
	public static class TotalPerMonthReducer extends Reducer<Text, Text, Text, Text> {
	
		@Override
		public void reduce(Text key_unit_cost_item, Iterable<Text> value_year_and_month_date, Context context) throws IOException, InterruptedException {
			
			Map<Integer, Integer> months = new TreeMap<Integer, Integer>();

			//il costo può essere composto da più cifre
			String [] parts =key_unit_cost_item.toString().split(" ");
			String item_unit_cost = parts[0];
			String item_name = parts[1];
			
			int item_unit_cost_val=Integer.parseInt(item_unit_cost);

			for (Text month : value_year_and_month_date) {
				StringTokenizer st=new StringTokenizer(month.toString(),"-");
				String res=	st.nextToken();	
				int month_int=Integer.parseInt(res);

				if (months.containsKey(month_int)) {
					// Map already contains the month. Just increment its count by unit cost of the item
					int sum=months.get(month_int)+item_unit_cost_val;
					months.put(month_int, sum);

				} else {
					// Map doesn't have mapping for month. Add one with count = cost of item
					months.put(month_int, item_unit_cost_val);
				}
			}

			String res="";
			String year="2015";

			for (Integer month : months.keySet()) {
				res=res+year+"-"+month+":"+months.get(month)+",\t";
			}
			Text result = new Text(res);
			Text item= new Text(item_name+"\t");
			context.write(item, result);
		}
	}
}
