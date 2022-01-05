/*
Submitted by -: Divya Gupta (dg3483)
1) I have run this program in single node hadoop cluster...
2) All the jobs will run in sequence on after the other.
3) We have to give only input director path and output directory name that hadoop will create as input for this program.
4) Firstly I have implemented one mapper and reducer class to pre-process all the 8 files by replacing all <p> with new line character (\n) and
removing all the commands in <cmd>. The output is single file store in /<output-directory name given in the input>/temp3 directory.
5) Then I have implemented 2 mapper and reducer classes for each unigram, bigram and trigram frequencies calculation.
    The output of unigram is store in /<output-directory name given in the input>/final1 directory
    the output of bigram is store in /<output-directory name given in the input>/final directory
    the output of trigram is store in /<output-directory name given in the input>/final2 directory.
6) then I have implemented three mapper and one reducer classes to merge all the outputs from trigram, bigram and unigrams in single file.
    This is store in directory /<output-directory name given in the input>/final3.

    Reference-:
    1) https://coe4bd.github.io/HadoopHowTo/multipleJobsSingle/multipleJobsSingle.html
    2) https://stackoverflow.com/questions/25480702/null-pointer-exception-in-hadoop-reducer
    3) http://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/lib/MultipleInputs.html
    4) http://hadooptutorial.info/mapreduce-use-case-for-n-gram-statistics/
    5) https://www.aegissofttech.com/articles/what-is-the-use-of-multiple-input-files-in-mapreduce-hadoop-development.html
*/




// namespace
package edu.nyu.bigdata;

//java dependencies
import java.io.IOException;
import java.util.*;
//import java.util.StringTokenizer;
import java.util.regex.*;

// hadoop dependencies
//  maven: org.apache.hadoop:hadoop-client:x.y.z  (x..z = hadoop version, 3.3.0 here
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Waitable;

import java.io.IOException;

//our program. This program will be 'started' by the Hadoop runtime
public class NgramCount {

    // this is the driver
    public static void main(String[] args) throws Exception {

        // get a reference to a job runtime configuration for this program
        Configuration conf = new Configuration();
        //
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        @SuppressWarnings("deprecation")
        // Jobs for pre-processing input files
        Job job7 = new Job(conf, "PreProcessing");
        job7.setJarByClass(NgramCount.class);
        job7.setMapperClass(PreMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job7.setReducerClass(PreReducer.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job7, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job7, new Path(otherArgs[1]+"/temp3"));
        job7.waitForCompletion(true);


        // Jobs for unigrams
        Job job2 = new Job(conf, "unigram count");
        job2.setJarByClass(NgramCount.class);
        job2.setMapperClass(UniMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job2.setReducerClass(UniReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/temp3"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/temp1"));
        job2.waitForCompletion(true);
        Job job3 = new Job(conf, "unigram count1");
        job3.setJarByClass(NgramCount.class);
        job3.setMapperClass(UniMapper1.class);
//        job.setCombinerClass(MyReducer.class);
        job3.setReducerClass(UniReducer1.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(otherArgs[1]+"/temp1"));
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1]+"/final1"));
        job3.waitForCompletion(true);

        // jobs for Bigram-:
        Job job = new Job(conf, "bigram count");
        job.setJarByClass(NgramCount.class);
        job.setMapperClass(BiMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(BiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]+"/temp3"));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"/temp"));
        job.waitForCompletion(true);
        Job job1 = new Job(conf, "bigram count1");
        job1.setJarByClass(NgramCount.class);
        job1.setMapperClass(BiMapper1.class);
//        job.setCombinerClass(MyReducer.class);
        job1.setReducerClass(BiReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]+"/temp"));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+"/final"));
        job1.waitForCompletion(true);

        // jobs for trigram

        Job job4 = new Job(conf, "trigram count");
        job4.setJarByClass(NgramCount.class);
        job4.setMapperClass(TriMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job4.setReducerClass(TriReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(otherArgs[1]+"/temp3"));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[1]+"/temp2"));
        job4.waitForCompletion(true);
        Job job5 = new Job(conf, "trigram count1");
        job5.setJarByClass(NgramCount.class);
        job5.setMapperClass(TriMapper1.class);
//        job.setCombinerClass(MyReducer.class);
        job5.setReducerClass(TriReducer1.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job5, new Path(otherArgs[1]+"/temp2"));
        FileOutputFormat.setOutputPath(job5, new Path(otherArgs[1]+"/final2"));
        job5.waitForCompletion(true);


        // jobs for merging data in single file.
        Job job6 = new Job(conf, "merge");
        job6.setJarByClass(NgramCount.class);
        job6.setMapperClass(MergeMapper.class);
//        job.setCombinerClass(MyReducer.class);
        job6.setReducerClass(MergeReducer.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job6, new Path(otherArgs[1]+"/final1"), TextInputFormat.class, MergeMapper.class);
        MultipleInputs.addInputPath(job6, new Path(otherArgs[1]+"/final"), TextInputFormat.class, MergeMapper1.class);
        MultipleInputs.addInputPath(job6, new Path(otherArgs[1]+"/final2"), TextInputFormat.class, MergeMapper2.class);
        FileOutputFormat.setOutputPath(job6, new Path(otherArgs[1]+"/final3"));
        System.exit(job6.waitForCompletion(true) ? 0 : 1);
    }

    // Below is the implementation of single mapper and reducer class for pre-processing files.
    public static class PreMapper extends Mapper<Object, Text, Text, Text> {
        private Text l= new Text();
        private Text k= new Text("");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line= value.toString().toLowerCase().replaceAll("<p>","\n").replaceAll("<.*>","");
            l.set(line);
            context.write(k, l);
        }
    }
    public static class PreReducer extends Reducer<Object,Text,Text,Text> {
        private Text k= new Text("");
        public void reduce(Object key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                context.write(k, val);
            }
        }
    }



    // Below is the implementation of two mapper and reducer classes for calculating likelihood of unigrams-:

    public static class UniMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        List<String> ls= new ArrayList<>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line= value.toString().toLowerCase().replaceAll("[^a-z0-9]"," ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                ls.add(itr.nextToken());
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            int va=1;
            StringBuffer str= new StringBuffer("");
            for(int i=0; i< ls.size(); i++) {
                int k=i;
                for(int j=0; j <va; j++)
                {
                    if(j>0)
                    {
                        str=str.append(" ");
                        str= str.append(ls.get(k));
                    }
                    else
                    {
                        str = str.append(ls.get(k));
                    }
                    k++;
                }
                word.set(str.toString());
                str = new StringBuffer("");
                context.write(word, one);
            }
        }
    }

    public static class UniReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class UniMapper1 extends Mapper<Object, Text, Text, Text> {

        Text word= new Text("Unigram:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class UniReducer1 extends Reducer<Text,Text,Text,Text> {

        Text prob= new Text();
        Text key= new Text();
        Integer sum=0;
        Hashtable<Text, Text> h1= new Hashtable<>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                StringTokenizer token= new StringTokenizer(val.toString());
                key= new Text(token.nextToken().toString());
                Integer v= Integer.parseInt(token.nextToken().toString());
                Text result=new Text(Integer.toString(v));
                h1.put(key, result);
                sum= sum+ v;
            }
            for(Map.Entry<Text, Text> e: h1.entrySet())
            {
                String s= e.getValue().toString()+"/"+Integer.toString(sum);
                prob.set(s);
                context.write(e.getKey(), prob);
            }
        }
    }


    // Below is 2 mapper and reducer implementations to output likelihood of bigrams...
    public static class BiMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        List<String> ls= new ArrayList<>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line= value.toString().toLowerCase().replaceAll("[^a-z0-9]"," ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                ls.add(itr.nextToken());
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            int va=2;
            StringBuffer str= new StringBuffer("");
            for(int i=0; i< ls.size()-1; i++) {
                int k=i;
                for(int j=0; j <va; j++)
                {
                    if(j>0)
                    {
                        str=str.append(" ");
                        str= str.append(ls.get(k));
                    }
                    else
                    {
                        str = str.append(ls.get(k));
                    }
                    k++;
                }
                word.set(str.toString());
                str = new StringBuffer("");
                context.write(word, one);
            }
        }
    }

    public static class BiReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class BiMapper1 extends Mapper<Object, Text, Text, Text> {

        Text word= new Text("Bigram:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class BiReducer1 extends Reducer<Text,Text,Text,Text> {

        Text prob= new Text();
        Text key= new Text();
        Integer sum=0;
        Hashtable<Text, Text> h1= new Hashtable<>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                StringTokenizer token= new StringTokenizer(val.toString());
                key= new Text(token.nextToken().toString()+ " "+ token.nextToken().toString());
                Integer v= Integer.parseInt(token.nextToken().toString());
                Text result=new Text(Integer.toString(v));
                h1.put(key, result);
                sum= sum+ v;
            }
            for(Map.Entry<Text, Text> e: h1.entrySet())
            {
                String s= e.getValue().toString()+"/"+Integer.toString(sum);
                prob.set(s);
                context.write(e.getKey(), prob);
            }
        }
    }

    //Below is the implementation of 2 mapper and reducer classes to calculate likelihood of trigrams
    public static class TriMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //static int cnt=0;
        List<String> ls= new ArrayList<>();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line= value.toString().toLowerCase().replaceAll("[^a-z0-9]"," ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                ls.add(itr.nextToken());
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            int va=3;
            StringBuffer str= new StringBuffer("");
            for(int i=0; i< ls.size()-2; i++) {
                int k=i;
                for(int j=0; j <va; j++)
                {
                    if(j>0)
                    {
                        str=str.append(" ");
                        str= str.append(ls.get(k));
                    }
                    else
                    {
                        str = str.append(ls.get(k));
                    }
                    k++;
                }
                word.set(str.toString());
                str = new StringBuffer("");
                context.write(word, one);
            }
        }
    }

    public static class TriReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TriMapper1 extends Mapper<Object, Text, Text, Text> {
        Text word= new Text("Trigram:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class TriReducer1 extends Reducer<Text,Text,Text,Text> {

        Text prob= new Text();
        Text key= new Text();
        Integer sum=0;
        Hashtable<Text, Text> h1= new Hashtable<>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                StringTokenizer token1= new StringTokenizer(val.toString());
                key= new Text(token1.nextToken().toString()+" "+token1.nextToken().toString()+" "+token1.nextToken().toString());
                String i= token1.nextToken().toString();
                Integer v= Integer.parseInt(i);
                Text result=new Text(Integer.toString(v));
                h1.put(key, result);
                sum= sum+ v;
            }
            for(Map.Entry<Text, Text> e: h1.entrySet())
            {
                String s= e.getValue().toString()+"/"+Integer.toString(sum);
                prob.set(s);
                context.write(e.getKey(), prob);
            }
        }
    }
    public static class MergeMapper extends Mapper<Object, Text, Text, Text> {
        Text word= new Text("UniGram Output:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class MergeMapper1 extends Mapper<Object, Text, Text, Text> {
        Text word= new Text("BiGram Output:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class MergeMapper2 extends Mapper<Object, Text, Text, Text> {
        Text word= new Text("TriGram Output:");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(word, value);
        }
    }
    public static class MergeReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val: values)
            {
                context.write(key, val);
            }
        }
    }

}


