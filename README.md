# BigDataAnalytics-DA1

# The Hadoop MapReduce program named DiscountCalculator ,processes customer purchase data, calculates discounted prices for purchases greater than 400, and outputs the original and discounted prices along with the customer name and ID.

# CODE:
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiscountCalculator {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");
            if (tokens.length >= 3) {
                String customerId = tokens[1];
                String customerName = tokens[0];
                int originalPrice = Integer.parseInt(tokens[2]);
                int discountedPrice = originalPrice;
                if (originalPrice > 400) {
                    // Apply 10% discount
                    discountedPrice = (int) (originalPrice * 0.9);
                }
                String outputValue = customerName + " " + originalPrice + " " + discountedPrice;
                context.write(new Text(customerId), new Text(outputValue));
            }
        }
    }

    public static class DiscountReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "discount calculator");

        job.setJarByClass(DiscountCalculator.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(DiscountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

* Input Format: 
The input file is expected to contain records where each line has the following format:
 ![image](https://github.com/user-attachments/assets/3db592bb-ac2c-43b8-b584-257067d6be83)


The Mapper applies a 10% discount to prices greater than 400 and outputs the customer ID as the key and customer details as the value.
The Reducer outputs the key-value pairs without modification.
The Main method configures the Hadoop job, specifies input/output paths, and runs the MapReduce job.

# How It Works??
For each customer, the program prints their name, ID, and amount spent. If the amount spent exceeds $200, the program calculates a 10% discount on the total amount and displays the discounted amount in parentheses.
The program utilizes a breadth-first search (BFS) algorithm to traverse a graph representing customer relationships. Each node in the graph represents a customer, and the edges represent connections between customers. The BFS algorithm starts from a specified customer node (e.g., customer with ID 0) and explores neighboring customers in a breadth-first manner. During traversal, the program prints customer information and applies discounts as needed.
