import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.io.StringReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class WordCount {
    public static int k = 0;
    public static class ToMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
            k=Integer.parseInt(conf.get("k"));
            if (conf.getBoolean("wordcount.skip.patterns", true)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] line = value.toString().split(" ");




            System.out.println(line.length);

            for (int j = 6; j<line.length-1; j++) {
                for (String pattern : patternsToSkip) {
                    line[j] = line[j].replaceAll(pattern, "");
                }
            }


            if (line.length >= 6) {
                String id = line[line.length-1].trim();
                for (int i = 6; i<line.length-1; i++) {
                    String content = line[i].trim();
                    StringReader sr = new StringReader(content);
                    IKSegmenter iks = new IKSegmenter(sr, true);
                    Lexeme lexeme = null;
                    while ((lexeme = iks.next()) != null) {
                        String word = lexeme.getLexemeText();
                        context.write(new Text(word), new IntWritable(1));
                    }
                    sr.close();
                }


            } else {
                System.err.println("error:" + value.toString() + "------------------------");
            }
        }


    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            k = Integer.parseInt(conf.get("k"));
        }

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if(sum>=k){
                result.set(sum);
                context.write(key, result);
            }

        }
    }

    public static class DescComparator extends WritableComparator {

        protected DescComparator() {
            super(IntWritable.class,true);
        }

        @Override
        public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
                           int arg4, int arg5) {
            return -super.compare(arg0, arg1, arg2, arg3, arg4, arg5);
        }
        @Override
        public int compare(Object a,Object b){
            return -super.compare(a, b);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 3 || remainingArgs.length != 5)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile] k");
            System.exit(2);
        }
        k= Integer.parseInt(remainingArgs[4]);
        conf.setInt("k", k);
        Path tempDir = new Path("wordcount-temp-" + Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        Job job = Job.getInstance(conf, "word count");
        /*job.setJarByClass(WordCount.class);
        job.setMapperClass(ToMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setSortComparatorClass(DescComparator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);*/
        try{
            job.setMapperClass(ToMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            List<String> otherArgs = new ArrayList<String>();
            for (int i=0; i < remainingArgs.length; ++i) {
                if ("-skip".equals(remainingArgs[i])) {
                    job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                } else {
                    otherArgs.add(remainingArgs[i]);
                }
            }

            FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
            FileOutputFormat.setOutputPath(job, tempDir);//先将词频统计任务的输出结果写到临时目
            //录中, 下一个排序任务以临时目录为输入目录。
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            if(job.waitForCompletion(true))
            {

                Job sortJob = Job.getInstance(conf, "sort");
                sortJob.setJarByClass(WordCount.class);

                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);

                /*InverseMapper由hadoop库提供，作用是实现map()之后的数据对的key和value交换*/
                sortJob.setMapperClass(InverseMapper.class);
                /*将 Reducer 的个数限定为1, 最终输出的结果文件就是一个。*/
                sortJob.setNumReduceTasks(1);


                FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));

                sortJob.setOutputKeyClass(IntWritable.class);
                sortJob.setOutputValueClass(Text.class);
                /*Hadoop 默认对 IntWritable 按升序排序，而我们需要的是按降序排列。
                 * 因此我们实现了一个 IntWritableDecreasingComparator 类,　
                 * 并指定使用这个自定义的 Comparator 类对输出结果中的 key (词频)进行排序*/
                sortJob.setSortComparatorClass(DescComparator.class);

                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
            }
        }finally{
            FileSystem.get(conf).deleteOnExit(tempDir);
        }


        /*FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}
