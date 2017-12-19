import java.io.BufferedReader;
import java.util.Scanner;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class bayes {
        public static String outtmp="";
        public static class TrainMapper
                extends Mapper<Object, Text, Text, Text>
        {

            static Text one = new Text("1");
            private Text word;


            public void map(Object key, Text value, Context context)
                    throws IOException, InterruptedException
            {
                word=new Text();

                String sentence = value.toString();
                String[] line = sentence.split("\t");
                String name=line[1];
                word.set(name);
                context.write(word,one);
                String[] str=line[0].split(" ");
                for(int i=0;i<str.length;i++){
                    Text word1=new Text();
                    String tmp="";
                    System.out.println();
                    tmp+=name+" ";
                    int k=i+1;
                    tmp+=k+" ";
                    tmp+=str[i];
                    word1.set(tmp);
                    context.write(word1,one);

                }

            }
        }

        public static class TrainReducer
                extends Reducer<Text,Text,Text,Text>
        {
            private IntWritable result = new IntWritable();
            public void reduce(Text key, Iterable<Text> values,
                               Context context) throws IOException, InterruptedException
            {
                float sum = 0;
                for (Text val : values)
                {
                    sum += Integer.parseInt(val.toString());
                }


                String value="";
                value+=sum;

                context.write(key, new Text(value));
            }
        }
    public static class TestMapper
            extends Mapper<Object, Text, Text, Text>
    {
        public float[] FY=new float [3];

        List<String> valstr = new ArrayList<String>();
        protected void setup(Context context)throws IOException, InterruptedException {
            FileSystem fileSystem = null;
            try {
                fileSystem = FileSystem.get(URI.create(outtmp), new Configuration());
            } catch (Exception e) {
            }
            FSDataInputStream fr0 = fileSystem.open(new Path(outtmp));
            BufferedReader fr1 = new BufferedReader(new InputStreamReader(fr0));

            String str = fr1.readLine();
            int i=0;
            while (str != null) {
                String[] line=str.split("\t");
                if(line[0].length()<3){
                    FY[i]=Float.parseFloat(line[1]);
                    i++;
                }else{
                    valstr.add(str);


                }

                str = fr1.readLine();
            }
        }


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            Scanner scan =new Scanner(value.toString());
            String str, vals[], temp;
            float fxyi,fyi,fyij,maxf;
            int idx;
            while(scan.hasNextLine()) {
                String sentence = scan.nextLine();
                String[] line = sentence.split("\t");
                String classy = line[1];
                vals = line[0].split(" ");
                String name = vals[vals.length - 1];//测试集名称
                maxf = -100;
                idx = -1;
                for (int i = 0; i < FY.length; i++) {
                    fxyi = 1;
                    String cl = "";
                    int num_l = i + 1;
                    cl += num_l;
                    fyi = FY[i];
                    for (int j = 0; j < 45; j++) {
                        for(int k=0;k<valstr.size();k++){
                            String sub=valstr.get(k);
                            String[] sline=sub.split("\t");
                            String[] test=sline[0].split(" ");
                            if((Integer.parseInt(test[0])==i+1 )
                                    && (Integer.parseInt(test[1])==j+1)
                                    && (Math.abs(Float.parseFloat(test[2])-Float.parseFloat(vals[j]))<0.00001)){
                                System.out.println(sline[0]);

                                fyij=Float.parseFloat(sline[1]);
                                System.out.println(fyij);

                                fxyi = fxyi * fyij/10;

                            }


                        }
                        //System.out.println(fxyi);
                        System.out.println(maxf);

                    }
                    if (fyi * fxyi > maxf) {
                        maxf = fyi * fxyi;
                        idx = i;
                    }

                }
                idx++;
                String putin = "";
                putin += idx;
                context.write(new Text(name), new Text(putin));
            }

        }
    }




    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bayes_p1");
        job.setJarByClass(bayes.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));//在这里指定输入文件的父目录即可，MapReduce会自动读取输入目录下所有的文件
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(TrainMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setNumReduceTasks(1);


        job.setReducerClass(TrainReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
        outtmp+=args[1];
        outtmp+="/part-r-00000";

        Configuration conf2 = new Configuration();

        Job job_test = Job.getInstance(conf2, "bayes_p2");
        job_test.setJarByClass(bayes.class);
        job_test.setMapperClass(TestMapper.class);

        job_test.setOutputKeyClass(Text.class);
        job_test.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job_test, new Path(args[2]));
        FileOutputFormat.setOutputPath(job_test, new Path(args[3]));
        if(job_test.waitForCompletion(true) == false)
            System.exit(1);


    }


}
