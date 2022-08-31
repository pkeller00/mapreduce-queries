import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.math.BigDecimal;
import java.math.RoundingMode;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

public class Main {

  /**
  * MapReduce - Mapper. Creates {key,value} pair which is then passed onto the Reducer. 
  *
  * @interface  Object database object. Data on which the queries are run. 
  * @interface  Text row from database file
  * @interface  IntWritable ss_sold_date acts as key.
  * @interface  DoubleWritable total ss_net_paid_inc_tax for given key.
  */
  public static class SumMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

    /*
    HashMap which acts as a local combiner. Groups values for ss_net_paid_inc_tax by ss_sold_date to 
    minimise data passed to reducer
    */
    HashMap<Integer, Double> totalNetTax = new HashMap<Integer, Double>();

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
      final int net_paid_inc_tax = 21;
      boolean is_null = (tokens[sold_date].isEmpty()) || ( tokens[net_paid_inc_tax].isEmpty());

      // If all relevant features exist, check against predefined criteria.
      if(!is_null){

        // Creates variable for each token in the record.
        Integer ss_sold_date_sk = Integer.parseInt(tokens[sold_date]);
        Double ss_net_paid_inc_tax = Double.parseDouble(tokens[net_paid_inc_tax]);

        // Determine date limits based on user's arguments.
        Configuration conf = context.getConfiguration();
        Integer start_date = Integer.parseInt(conf.get("start_date"));
        Integer end_date = Integer.parseInt(conf.get("end_date"));

        if((ss_sold_date_sk >= start_date) && (ss_sold_date_sk <= end_date)){
          // If key in the HashTable does not exist add as new.
          if(totalNetTax.get(ss_sold_date_sk) == null){
            totalNetTax.put(ss_sold_date_sk, ss_net_paid_inc_tax);
          }
          // Otherwise, add to the value of existing key.
          else{
            totalNetTax.put(ss_sold_date_sk,  totalNetTax.get(ss_sold_date_sk) + ss_net_paid_inc_tax);
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
      for (Integer key : totalNetTax.keySet()) {
          context.write(new IntWritable(key),new DoubleWritable(totalNetTax.get(key)));
      } 
    }

  }

  /**
  * Reducer function. Calculates how much total net paid we have per store.
  *
  * @interface  IntWritable ss_sold_date acts as key.
  * @interface  DoubleWritable total ss_net_paid_tax_inc_tax for given key.
  * @interface  Text ss_sold_date acts as key.
  * @interface  DoubleWritable total ss_net_paid_tax_inc_tax for given key.
  */
  public static class SumReducer extends Reducer<IntWritable,DoubleWritable,Text,DoubleWritable> {
    
    //Stores all stores in a red-black tree. Only keeps the top k stores removing all other nodes.
    private TreeMap<Double, Text> TopKMap = new TreeMap<Double, Text>() ;

    /**
     * Rounds a double to a specified number of places.
     * @param value number to round.
     * @param places number of places to round to.
     * @return rounded number.
     */
    public double round(double value, int places){

      BigDecimal bd = BigDecimal.valueOf(value);
      bd = bd.setScale(places, RoundingMode.HALF_UP);
      
  
      return bd.doubleValue();
    }

    /**
     * Function to sum all ss_quantity per ss_item_sk.
     * 
     * @param key ss_sold_date acts as key.
     * @param values all ss_net_paid_tax_inc_tax for a given key.
     * @param context MapReduce next pipeline stage.
     */
    public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();
      Integer K = Integer.parseInt(conf.get("K"));//get how many dates user wants

      double sum = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
      }


      TopKMap.put(new Double(sum),new Text(key.toString()));

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
      for (Double key : TopKMap.descendingKeySet()) {
          context.write(TopKMap.get(key),new DoubleWritable(this.round(key,2)));
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

    Job job = Job.getInstance(conf1, "tax max");

    job.setJarByClass(Main.class);
    job.setMapperClass(SumMapper.class);
    job.setReducerClass(SumReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[3]));
    FileOutputFormat.setOutputPath(job, new Path(args[4]));

      
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}

