package steps.firstStep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class FirstStep {

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
            // value line format is : ngram TAB year TAB match_count TAB volume_count NEWLINE
            String[] lineData = value.toString().split("\t");
            String[] ngram = lineData[0].split(" ");
            if (ngram.length != 3) {
                return;
            }
            //Replacing all non-alphanumeric characters with empty strings
            String firstWord = ngram[0];
            String secondWord = ngram[1];
            String thirdWord = ngram[2];
            LongWritable appearances = new LongWritable(Long.parseLong(lineData[2]));

            Text initialKey = new Text("a" + "\t" + "*" + "\t" + "*" + "\t" + "*"); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("a" + "\t" + firstWord + "\t" + "*" + "\t" + "*"); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("b" + "\t" + "*" + "\t" + secondWord + "\t" + "*"); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("b" + "\t" + "*" + "\t" + "*" + "\t" + thirdWord); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("c" + "\t" + firstWord + "\t" + secondWord + "\t" + "*"); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("c" + "\t" + "*" + "\t" + secondWord + "\t" + thirdWord); //N3
            output.write(initialKey, appearances);

            initialKey = new Text("d" + "\t" + firstWord + "\t" + secondWord + "\t" + thirdWord); //N3
            output.write(initialKey, appearances);
        }
    }


    public static class ReduceForWordCount extends Reducer<Text, LongWritable, Text, DoubleWritable> {
        private double C0Value;
        private final HashMap<String, Double> C1Map = new HashMap<>();
        private final HashMap<String, Double> C2Map = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context con) throws IOException, InterruptedException {
            double occurrences = 0;
            for (LongWritable value : values) {
                occurrences += value.get();
            }
            String[] words = key.toString().split("\t");

            String firstWord = words[1];
            String secondWord = words[2];
            String thirdWord = words[3];

            boolean total = firstWord.equals("*") && secondWord.equals("*") && thirdWord.equals("*");
            boolean oneWord = firstWord.equals("*") && secondWord.equals("*") || firstWord.equals("*") && thirdWord.equals("*") || thirdWord.equals("*") && secondWord.equals("*");
            boolean couple = thirdWord.equals("*") || firstWord.equals("*");
            boolean trio = !firstWord.equals("*") && !secondWord.equals("*") && !thirdWord.equals("*");

            if (total) {
                C0Value = occurrences * 3;
            } else if (oneWord) {
                String word = (!firstWord.equals("*") ? firstWord : !secondWord.equals("*") ? secondWord : thirdWord);
                if (!C1Map.containsKey(word)) {
                    C1Map.put(word, 0D);
                }
                C1Map.put(word, C1Map.get(word) + occurrences);
            } else if (couple) {
                String pair = (thirdWord.equals("*") ? firstWord + "," + secondWord : secondWord + "," + thirdWord);
                if (!C2Map.containsKey(pair)) {
                    C2Map.put(pair, 0D);
                }
                C2Map.put(pair, C2Map.get(pair) + occurrences);
            } else if (trio) {
                DoubleWritable outProb = new DoubleWritable();
                double N2 = C2Map.get(secondWord + "," + thirdWord);
                double K2 = (Math.log(N2 + 1) + 1D) / (Math.log(N2 + 1D) + 2D);
                double K3 = (Math.log(occurrences + 1D) + 1D) / (Math.log(occurrences + 1) + 2D);
                double C1 = C1Map.get(secondWord);
                double C2 = C2Map.get(firstWord + "," + secondWord);
                double N1 = C1Map.get(thirdWord);
                double prob = (K3 * (occurrences / C2)) + ((1D - K3) * K2 * (N2 / C1)) + ((1D - K3) * (1D - K2) * (N1 / C0Value));

                outProb.set(prob);
                con.write(new Text(firstWord + " " + secondWord + " " + thirdWord), outProb);
            }
        }
    }

    public static class CombinerClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        private final LongWritable occurrences = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long newOccurrences = 0;
            for (LongWritable value : values) {
                newOccurrences += value.get();
            }
            this.occurrences.set(newOccurrences);
            context.write(key, this.occurrences);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path("s3://program-bucket-28031995/FirstLevelOutput/");

        String uuid = args[1];

        String tempFilesPath = "program-bucket-28031995/tempFiles/" + uuid;
        conf.set("tempFilesPath", tempFilesPath);

        Job job = Job.getInstance(conf, "FirstLevel");
        job.setJarByClass(FirstStep.class);
        job.setMapperClass(MapForWordCount.class);
        job.setReducerClass(ReduceForWordCount.class);
        job.setCombinerClass(CombinerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);//TextInputFormat.class
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
