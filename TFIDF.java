import java.io.IOException;
import java.io.StringReader;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class TFIDF {

    public static String[] Array=new String[45];
    public static float[] IDF=new float[45];
    public static int isHave(String[] strs,String s){
/*此方法有两个参数，第一个是要查找的字符串数组，第二个是要查找的字符或字符串
* */
        for(int i=0;i<strs.length;i++){
            if(strs[i].indexOf(s)!=-1){//循环查找字符串数组中的每个字符串中是否包含所有查找的内容
                return i;//查找到了就返回真，不在继续查询
            }
        }
        return -1;//没找到返回false
    }
    // part1------------------------------------------------------------------------
    public static class Mapper_Part1 extends
            Mapper<LongWritable, Text, Text, Text> {
        String File_name = ""; // 保存文件名，根据文件名区分所属文件

        static Text one = new Text("1");
        String word;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String str =  ((FileSplit) inputSplit).getPath().toString();
            File_name = str.substring(str.lastIndexOf("/") + 1); // 获取文件名
            byte[] valueBytes = value.getBytes();
            String sentence = new String(valueBytes, "GBK");
            String[] line = sentence.split(" ");
            if (line.length >= 2) {

                for (int i = 2; i<line.length; i++) {
                    String content = line[i].trim();

                    StringReader sr = new StringReader(content);
                    IKSegmenter iks = new IKSegmenter(sr, true);
                    Lexeme lexeme = null;
                    while ((lexeme = iks.next()) != null) {
                        String word = File_name;
                        word += " ";
                        String tmp= lexeme.getLexemeText();
                        int index=isHave(Array,tmp);
                        if(index>0) {
                            word+=tmp;
                            word+=' ';
                            word+=IDF[index];
                            context.write(new Text(word), one);

                        }

                    }
                    sr.close();

                } }else {
                System.err.println("error:" + value.toString() + "------------------------");
            }

        }



    }

    public static class Combiner_Part1 extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {


            float sum = 0; // 统计某个单词出现的次数
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }

            String value = "";
             // 记录词频

            // 将key中单词和文件名进行互换。es: test1 hello -> hello test1
            String p[] = key.toString().split(" ");
            String key_to = "";
            key_to += p[0];
            key_to += " ";
            key_to += p[1];


            float idf=Float.parseFloat(p[2]);
            value+=sum*idf;


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
            String s1 = val.substring(0, index); // 获取文本
            String s2 = val.substring(index + 1); // 其余部分 作为value es: hello
            // 0.11764706

            context.write(new Text(s1), new Text(s2));
        }
    }

    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text> {
        static Text one = new Text("1");

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 同一个单词会被分成同一个group
            float sum = 0;
            List<String> vals = new ArrayList<String>();
            for (Text str : values) {
                vals.add(str.toString()); // 保存

            }
            float[] mem=new float[Array.length];
            for(int i=0;i<Array.length;i++){
                mem[i]=0;
            }

            for (int j = 0; j < vals.size(); j++) {
                String val = vals.get(j);
                String[] str=val.split("\t");
                String word = str[0].trim();
                System.out.println(word);
                String end = str[1].trim();
                float f_end = Float.parseFloat(end); // 读取TF
                int index=isHave(Array,word);
                if (index>=0){
                    mem[index]=f_end;
                }

            }
            String value="";
            for(int i=0;i<Array.length;i++){
                value+=mem[i];
                value+=" ";
            }
            context.write(new Text(value), new Text(one));
        }
    }

    public static void main(String[] args) throws Exception {
        // part1----------------------------------------------------
        Configuration conf = new Configuration();

        BufferedReader bufferedReader = null;
        FSDataInputStream fsr = null;
        String lineTxt = null;
        int i=0;
        try {
            FileSystem fs = FileSystem.get(URI.create("/special/special"), conf);
            fsr = fs.open(new Path("/special/special"));
            bufferedReader = new BufferedReader(new InputStreamReader(fsr));
            while ((lineTxt = bufferedReader.readLine()) != null) {
                System.out.println("hello");
                String[] str=lineTxt.split("\t");
                Array[i] = str[1].trim();
                IDF[i] = Float.parseFloat(str[0].trim());
                i++;

            }
            System.out.println(Array);
            System.out.println(IDF);
        }catch (Exception e)
        {
            e.printStackTrace();
        }



        Configuration conf1 = new Configuration();
        // 设置文件个数，在计算DF(文件频率)时会使用
        FileSystem hdfs = FileSystem.get(conf1);
        FileStatus p[] = hdfs.listStatus(new Path(args[0]));

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

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
        // part2----------------------------------------
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "My_tdif_part2");
        job2.setJarByClass(TFIDF.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(Mapper_Part2.class);
        job2.setReducerClass(Reduce_Part2.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);

//        hdfs.delete(new Path(args[1]), true);
    }
}