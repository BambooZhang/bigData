import java.io.IOException;

import entity.DataTotalWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * MapReduce复核类型数据处理
 *
  * <p>源码出自https://blog.csdn.net/helpless_pain/article/details/7015383</p>
 * 拷贝人 bamoboo
 * 2018-9-14
 */


public class Hadoop3  extends Configured implements Tool {

    //输入资源映射处理
    public static class DataTotalMapper extends
            Mapper<LongWritable, Text, Text, DataTotalWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //split by '\t'
            String[] splits = value.toString().split("\t") ;

            //以手机号码作为output key
            String phoneNum = splits[0];
            Text mapOutputKey = new Text();
            mapOutputKey.set(phoneNum);

            // set map output value
            long upPackNum = Long.parseLong(splits[1]) ;
            long downPackNum = Long.parseLong(splits[2]) ;
            long upPayLoad = Long.parseLong(splits[3]) ;
            long downPayLoad = Long.parseLong(splits[4]) ;
            DataTotalWritable mapOutputValue = new DataTotalWritable() ;
            mapOutputValue.set(upPackNum, downPackNum, upPayLoad, downPayLoad);

            //map output
            context.write(mapOutputKey, mapOutputValue);
        }
    }

    public static class DataTotalReducer extends
            Reducer<Text, DataTotalWritable, Text, DataTotalWritable> {

        @Override
        protected void reduce(Text key, Iterable<DataTotalWritable> values,
                              Context context) throws IOException, InterruptedException {

            long upPackNumSum = 0;
            long downPackNumSum = 0;
            long upPayLoadSum = 0;
            long downPayLoadSum = 0;

            //iterator
            for(DataTotalWritable value : values){
                upPackNumSum += value.getUpPackNum() ;
                downPackNumSum += value.getDownPackNum() ;
                upPayLoadSum += value.getUpPayLoad() ;
                downPayLoadSum += value.getDownPayLoad()  ;
            }

            // set output value
            DataTotalWritable outputValue = new DataTotalWritable() ;
            outputValue.set(upPackNumSum, downPackNumSum, upPayLoadSum, downPayLoadSum);

            // output
            context.write(key, outputValue);
        }
    }

    public int run(String[] args) throws Exception {

        //Job
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());

        //Mapper
        job.setMapperClass(DataTotalMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataTotalWritable.class);

        //Reducer
        job.setReducerClass(DataTotalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataTotalWritable.class);

        //输入路径
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        //输出路径
        Path outPath = new Path(args[1]);
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(outPath)) {
            dfs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //Submit Job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }


    //这里的运行输入和输出已经在程序中写死了不需要在执行时指定参数
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();


        //运行时设置输入 输出
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //如果args的参数长度小于2直接退出程序
        if (otherArgs.length < 2) {
            System.err.println("Usage: EventCount <in> <out>");
            System.exit(2);
        }

        //也可以直接写死
        //args = new String[] {"hdfs://blue01.mydomain:8020/input2", "hdfs://blue01.mydomain:8020/output2"};

        // run job
        int status = ToolRunner.run(conf,new Hadoop3(),args);

        System.exit(status);

       /* Long p=13123412431L;
        for (int i=0;i<10;i++){
            System.out.printf("%d\t%d\t%d\t%d\t%d\n",p++ ,38+i, 23+i,  24+i, 42+i);
        }*/
    }
}
