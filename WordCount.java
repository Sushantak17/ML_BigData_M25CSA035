import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper: emits (word,1)
    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

            String line = value.toString().replaceAll("[^a-zA-Z ]", " ");
            StringTokenizer itr = new StringTokenizer(line);

            while (itr.hasMoreTokens()) {
                Text word = new Text(itr.nextToken().toLowerCase());
                context.write(word, new IntWritable(1));
            }
        }

    }

    // Reducer: sums counts
    public static class Reduce
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                   Context context)
            throws IOException, InterruptedException {

            int sum = 0;

            // Sum all values associated with the word
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Emit final word count
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count example");

        job.getConfiguration().setLong(
            "mapreduce.input.fileinputformat.split.maxsize",
            67108864
        );
        job.setJarByClass(WordCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        long startTime = System.currentTimeMillis();

        boolean status = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();

        System.out.println("Execution time: " + (endTime - startTime) + " ms");

        System.exit(status ? 0 : 1);
}

}

