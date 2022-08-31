import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.IntWritable;

public class Main {

 
  /**
  * MapReduce - Mapper. Creates {key,value} pair which is then passed onto the Reducer. 
  *
  * @interface  Object database object. Data on which the queries are run. 
  * @interface  Text row from database file
  * @interface  Text ss_item_sk acts as key.
  * @interface  IntWritable total ss_quantity for given key.
  */
  public static class SumMapper extends Mapper<Object, Text, Text, IntWritable>{

    /*
    HashMap which acts as a local combiner. Groups values for ss_quantity by ss_item_sk to 
    minimise data passed to reducer
    */
    HashMap<Text, Integer> netItemSales = new HashMap<Text, Integer>();

    /**
    * Mapper function. Retrieves relevant features; matches date range criteria; ignores NULL entries. 
    * Uses a hashmap to reduce amount of data that is being passed to the Reducer.
    *
    * @param key Object key.
    * @param value entry in the database.
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      
      String[] tokens;
      tokens = line.split("\\|",-1);//tokinising an entry spliting by '|'.

      // Ensuring none of the relevant features are null.
      final int sold_date = 0;
      final int item_sk = 2;
      final int quantity = 10;
      boolean is_null = (tokens[sold_date].isEmpty()) || (tokens[item_sk].isEmpty()) || ( tokens[quantity].isEmpty());
      
      // If all relevant features exist, check against predefined criteria.
      if(!is_null){

        // Creates variable for each token in the record.
        Integer ss_sold_date_sk = Integer.parseInt(tokens[sold_date]);
        Text ss_item_sk = new Text(tokens[item_sk]);
        Integer ss_quantity = Integer.parseInt(tokens[quantity]);

        // Determine date limits based on user's arguments.
        Configuration conf = context.getConfiguration();
        Integer start_date = Integer.parseInt(conf.get("start_date"));
        Integer end_date = Integer.parseInt(conf.get("end_date"));

        if((ss_sold_date_sk >= start_date) && (ss_sold_date_sk <= end_date)){
          // If key in the HashTable does not exist add as new.
          if(netItemSales.get(ss_item_sk) == null){
            netItemSales.put(ss_item_sk, ss_quantity);
          }
          // Otherwise, add to the value of existing key.
          else{
            netItemSales.put(ss_item_sk,  netItemSales.get(ss_item_sk) + ss_quantity);
          }
        }
      }  
    }

    /**
    * Mapper's cleanup function. Context writes values from the HashMap. 
    *
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Text key : netItemSales.keySet()) {
          context.write(key,new IntWritable(netItemSales.get(key)));
      } 
    }

  }

  /**
  * Reducer function. Calculates how much total net paid we have per store.
  *
  * @interface  Text ss_item_sk acts as key.
  * @interface  IntWritable total ss_quantity for given key.
  * @interface  Text ss_item_sk acts as key.
  * @interface  IntWritable total ss_quantity for given key.
  */
  public static class SumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    
    //Stores all stores in a red-black tree. Only keeps the top k stores removing all other nodes.
    private TreeMap<Integer, Text> TopKMap = new TreeMap<Integer, Text>() ;

    /**
     * Function to sum all ss_quantity per ss_item_sk.
     * 
     * @param key ss_item_sk acts as key.
     * @param values all ss_quantity for a given key.
     * @param context MapReduce next pipeline stage.
     */
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      Integer K = Integer.parseInt(conf.get("K"));//get how many items user wants

      int sum = 0;
      
      for (IntWritable val : values) {
        sum += val.get();
      }

      TopKMap.put(new Integer(sum),new Text(key));

      //If we have more than K elements in tree than remove smallest one as we don't need it
      //this ensures tree doesn't store unecessary data and thus get too big.
      if (TopKMap.size() > K) {
          TopKMap.remove(TopKMap.firstKey());
      }
            
    }
    
    /**
    * Reducer's cleanup function. Context writes values from the tree. 
    *
    * @param context MapReduce next pipeline stage.
    * @return None.
    */
    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (Integer key : TopKMap.descendingKeySet()) {
          context.write(TopKMap.get(key),new IntWritable(key));
      }   
    }
  
  }


  /**
   * Sets config for mapreduce job
   * @param args input args to the file
   * @return None
   */
  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();

    conf1.set("K", args[0]);
    conf1.set("start_date", args[1]);
    conf1.set("end_date", args[2]);

    Job job = Job.getInstance(conf1, "sales count");

    job.setJarByClass(Main.class);
    job.setMapperClass(SumMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(args[4]));

              
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}

