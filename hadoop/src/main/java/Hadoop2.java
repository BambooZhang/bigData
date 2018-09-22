import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * MapReduce作业:功能和wordcount一样，在运行jar时设置文件输源和结果输出源
 *
 * 补充：
 * 设置命令行参数变量来编程
 　　这里需要借助Hadoop中的一个类Configured、一个接口Tool、ToolRunner（主要用来运行Tool的子类也就是run方法）

 * <p>源码出自https://blog.csdn.net/yinbucheng/article/details/70243593</p>
 * 拷贝人bamoboo
 * 2018-9-17
 */


public class Hadoop2 {

    //输入资源映射处理
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        private Text event = new Text();//字符串
        private final static IntWritable one = new IntWritable(1);//默认字符串的个数为1
        //字符串的分隔
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int idx = value.toString().indexOf(" ");//空格的位置
            if (idx > 0) {
                String e = value.toString().substring(0, idx);//
                event.set(e);
                context.write(event, one);//反正结果<e,1>
            }
        }
    }

    //对输出结果集处理
    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        //把多个map中返回的<e,1>合并<key,values>即<e,[1,1,1]>后遍历
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {//遍历相同key的所有结果集,这里保存的是int计算结果和
                sum += val.get();
            }
            result.set(sum);//把结果放入int
            context.write(key, result);//注意：在map或reduce上面的打印语句是没有办法输出的，但会记录到日志文件当中。
        }
    }

    /**
     *  args 在运行jar时设置参数
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //如果args的参数长度小于2直接退出程序
        if (otherArgs.length < 2) {
            System.err.println("Usage: EventCount <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "event count");//构建作业配置job

        //设置该作业所要执行的类
        job.setJarByClass(Hadoop2.class);

        job.setMapperClass(MyMapper.class);//设置map
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(MyReducer.class);//设置

        //设置自定义的Reducer类以及输出时的类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//设置文件输入源的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置结果输出流的路径


        System.exit(job.waitForCompletion(true) ? 0 : 1);//提交作业完成后退出
    }
}
