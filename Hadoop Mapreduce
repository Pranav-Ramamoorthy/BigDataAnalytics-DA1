package pranav;

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
