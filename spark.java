import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class WordCount {
    public static ArrayList<String> anaylyzerWords(String str)throws Exception{
        ArrayList<String> list=new ArrayList<String>();
        StringReader sr = new StringReader(str);
        IKSegmenter iks = new IKSegmenter(sr, true);
        Lexeme lexeme = null;
        while ((lexeme = iks.next()) != null) {

            list.add(lexeme.getLexemeText());

        }
        sr.close();
        return list;
    }

    public static void main(String[] args) throws Exception{
        SparkSession spark = SparkSession.builder().appName("JavaWordCount").master("local").getOrCreate();
        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> anaylyzerWords(s).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());

        }
    }
}

