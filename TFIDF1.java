import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class TFIDF {
    // part1------------------------------------------------------------------------


    public static class Mapper_Part1 extends
            Mapper<LongWritable, Text, Text, Text> {
        private Set<String> patternsToSkip = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;
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

        public void setup(Mapper.Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("skip.patterns", true)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        String File_name = ""; // 保存文件名，根据文件名区分所属文件
        int all = 0; // 单词总数统计
        static Text one = new Text("1");
        String word;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String str =  ((FileSplit) inputSplit).getPath().toString();
            String[] nameSegments = str.split("/");
            File_name = nameSegments[nameSegments.length - 1]; // 获取文件名
            /*StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word = File_name;
                word += " ";
                word += itr.nextToken(); // 将文件名加单词作为key es: test1 hello 1
                all++;
                context.write(new Text(word), one);
            }*/
            byte[] valueBytes = value.getBytes();
            String sentence = new String(valueBytes, "GBK");
            String[] line = sentence.split(" ");






            for (int j = 1; j<line.length-1; j++) {
                for (String pattern : patternsToSkip) {
                    line[j] = line[j].replaceAll(pattern, "");
                }
            }


            if (line.length >= 2) {

                for (int i = 2; i<line.length; i++) {
                    String content = line[i].trim();

                        StringReader sr = new StringReader(content);
                        IKSegmenter iks = new IKSegmenter(sr, true);
                        Lexeme lexeme = null;
                        while ((lexeme = iks.next()) != null) {
                            String word = File_name;
                            word += " ";
                            word += lexeme.getLexemeText();
                            all++;
                            context.write(new Text(word), one);

                        }
                        sr.close();

                } }else {
                System.err.println("error:" + value.toString() + "------------------------");
            }
        }

        public void cleanup(Context context) throws IOException,
                InterruptedException {
            // Map的最后，我们将单词的总数写入。下面需要用总单词数来计算。
            String str = "";
            str += all;
            context.write(new Text(File_name + " " + "!"), new Text(str));
            // 主要这里值使用的 "!"是特别构造的。 因为!的ascii比所有的字母都小。
        }
    }

    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {
        float all = 0;
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int index = key.toString().indexOf(" ");
            // 因为!的ascii最小，所以在map阶段的排序后，!会出现在第一个
            if (key.toString().substring(index + 1, index + 2).equals("!")) {
                for (Text val : values) {
                    // 获取总的单词数。
                    all = Integer.parseInt(val.toString());
                }
                // 这个key-value被抛弃
                return;
            }
            float sum = 0; // 统计某个单词出现的次数
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            // 跳出循环后，某个单词数出现的次数就统计完了，所有 TF(词频) = sum / all
            float tmp = sum / all;
            String value = "";
            value += tmp; // 记录词频

            // 将key中单词和文件名进行互换。es: test1 hello -> hello test1
            String p[] = key.toString().split(" ");
            String key_to = "";
            key_to += p[1];
            //System.out.println(p[1]);
            //System.out.println(p[0]);
            key_to += " ";
            key_to += p[0];
            //System.out.println(key_to);
            context.write(new Text(key_to), new Text(value));
        }
    }

    public static class Reduce_Part1 extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    public static class MyPartitoner extends Partitioner<Text, Text> {
        // 实现自定义的Partitioner
        public int getPartition(Text key, Text value, int numPartitions) {
            // 我们将一个文件中计算的结果作为一个文件保存
            // es： test1 test2
            String ip1 = key.toString();
            ip1 = ip1.substring(0, ip1.indexOf(" "));
            Text p1 = new Text(ip1);
            return Math.abs((p1.hashCode() * 127) % numPartitions);
        }
    }

    // part2-----------------------------------------------------
    public static class Mapper_Part2 extends
            Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String val = value.toString().replaceAll("    ", " "); // 将vlaue中的TAB分割符换成空格
            // es: Bank
            // test1
            // 0.11764706 ->
            // Bank test1
            // 0.11764706
            int index = val.indexOf(" ");
            String s1 = val.substring(0, index); // 获取单词 作为key es: hello
            String s2 = val.substring(index + 1); // 其余部分 作为value es: test1
            // 0.11764706
            s2 += " ";
            s2 += "1"; // 统计单词在所有文章中出现的次数, “1” 表示出现一次。 es: test1 0.11764706 1
            //System.out.println(s1);
            //System.out.println(s2);
            context.write(new Text(s1), new Text(s2));
        }
    }

    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text> {
        int file_count;

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 同一个单词会被分成同一个group
            file_count = context.getNumReduceTasks(); // 获取总文件数

            float sum = 0;
            List<String> vals = new ArrayList<String>();
            for (Text str : values) {
                int index = str.toString().lastIndexOf(" ");
                //System.out.println(index);
                //System.out.println(str.toString().substring(index + 1));
                sum += Integer.parseInt(str.toString().substring(index + 1)); // 统计此单词在所有文件中出现的次数
                vals.add(str.toString().substring(0, index)); // 保存
            }
            double tmp = Math.log10( file_count*1.0 /(sum*1.0));
            //System.out.println(tmp);// 单词在所有文件中出现的次数除以总文件数 = IDF
            for (int j = 0; j < vals.size(); j++) {
                String val = vals.get(j);
                //System.out.println(val);
                String end = val.substring(val.lastIndexOf("\t"));
                //System.out.println(end);
                float f_end = Float.parseFloat(end); // 读取TF

                val += " ";
                val += f_end * tmp; //  tf-idf值
                val=val.substring(val.lastIndexOf(" ") + 1);
                System.out.println(key);
                System.out.println(val);
                //System.out.println(file_count+'\n');
                context.write(key, new Text(val));
            }
        }
    }
    public static class IntSumReducer
            extends Reducer<Text,Text,Text,FloatWritable> {

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            //k = Integer.parseInt(conf.get("k"));
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float sum=0;
            for (Text value : values) {
                int index = value.toString().lastIndexOf(" ");
                System.out.println(value.toString().substring(index + 1));
                sum += Float.parseFloat(value.toString().substring(index + 1));
            }
            //if(sum>=k){
                //result.set(sum);

            //System.out.println(result);
                context.write(key, new FloatWritable(sum));
            //}

        }
    }
    public static class DescComparator extends WritableComparator {

        protected DescComparator() {
            super(FloatWritable.class,true);
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
    public static class Reduce_Part3 extends Reducer<FloatWritable, Text, FloatWritable, Text> {
        public void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int k=18;

            for (Text val : values) {
                    if(k>=0) {
                        context.write(key, val);
                    }
                k--;

                }



        }
    }

    public static void main(String[] args) throws Exception {
        // part1----------------------------------------------------
        Configuration conf1 = new Configuration();
        // 设置文件个数，在计算DF(文件频率)时会使用
        FileSystem hdfs = FileSystem.get(conf1);
        FileStatus p[] = hdfs.listStatus(new Path(args[0]));
        //Path tempDir = new Path("tfidf-temp-" + Integer.toString(
                //new Random().nextInt(Integer.MAX_VALUE)));

        // 获取输入文件夹内文件的个数，然后来设置NumReduceTasks
        Job job1 = Job.getInstance(conf1, "My_tdif_part1");
        job1.setJarByClass(TFIDF.class);
        job1.setMapperClass(Mapper_Part1.class);
        job1.setCombinerClass(Combiner_Part1.class); // combiner在本地执行，效率要高点。
        job1.setReducerClass(Reduce_Part1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setNumReduceTasks(p.length);
        job1.setPartitionerClass(MyPartitoner.class); // 使用自定义MyPartitoner
        job1.addCacheFile(new Path(args[4]).toUri());
        job1.getConfiguration().setBoolean("skip.patterns", true);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
        // part2----------------------------------------
        Configuration conf2 = new Configuration();
        Path tempDir = new Path("tfidf-temp-" + Integer.toString(
                new Random().nextInt(Integer.MAX_VALUE))); //定义一个临时目录
        Job job2 = Job.getInstance(conf2, "My_tdif_part2");

        job2.setJarByClass(TFIDF.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
        job2.setMapperClass(Mapper_Part2.class);
        job2.setCombinerClass(Reduce_Part2.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setNumReduceTasks(p.length);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, tempDir);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        job2.waitForCompletion(true);


        Configuration conf3 = new Configuration();
            Job sortJob = Job.getInstance(conf3, "sort");
            try{
                sortJob.setJarByClass(TFIDF.class);
                sortJob.setMapOutputKeyClass(FloatWritable.class);
                sortJob.setMapOutputValueClass(Text.class);


                FileInputFormat.addInputPath(sortJob, tempDir);
                sortJob.setInputFormatClass(SequenceFileInputFormat.class);


                sortJob.setMapperClass(InverseMapper.class);
                sortJob.setReducerClass(Reduce_Part3.class);
            //sortJob.setReducerClass(Reduce_Part3.class);

                sortJob.setNumReduceTasks(1);


                FileOutputFormat.setOutputPath(sortJob, new Path(args[2]));

                sortJob.setOutputKeyClass(FloatWritable.class);
                sortJob.setOutputValueClass(Text.class);

                sortJob.setSortComparatorClass(DescComparator.class);

                System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
             //FileSystem.get(conf1).deleteOnExit(tempDir);
        }
        finally{
            FileSystem.get(conf3).deleteOnExit(tempDir);
        }

        //FileSystem.get(conf1).deleteOnExit(tempDir);


//        hdfs.delete(new Path(args[1]), true);
    }
}
