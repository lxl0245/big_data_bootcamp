package com.geekbang;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataMRDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DataMRDriver.class);
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);

        // 设置 Map 输出数据的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataBean.class);

        // 设置 Reduce 输入数据的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DataBean.class);

        // 设置文件输入格式和输出格式
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 输入数据上传到HDFS
        // args[0] 输入数据文件
        // args[1] 输出结果的额存放目录
        // args[2] 存放从HDFS下载的文件的本地目录
        System.out.println("输入数据上传到HDFS");
        prepare(args[0], args[1]);

        // 提交 job
        job.setNumReduceTasks(1);
        job.waitForCompletion(true);

        // 从HDFS 上下载结果到本地
        System.out.println("从HDFS 上下载结果到本地");
        downloadFromHDFS(args[1], args[2]);
    }



    public static void prepare(String input_data, String output_dir) throws IOException {
        // String[] a = input_data.split("\\\\");
        String[] a = input_data.split(File.separator);
        String input_data_file = a[a.length-1];
        String input_data_dir = input_data.replace(input_data_file, "");

        // HDFS 创建文件夹
        // String input_dir = "/home/student5/lixinli/week02/mr_input_data";
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://emr-header-1.cluster-285604:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path(input_data_dir));
        System.out.println("目录:" + input_data_dir + ",创建成功");

        // 上传文件  HTTP_20130313143750.dat
        // fileSystem.copyFromLocalFile(new Path("file:////home/student5/lixinli/week02/mr_input_data/HTTP_20130313143750.dat"), new Path("/home/student5/lixinli/week02/mr_input_data/"));
        fileSystem.copyFromLocalFile(new Path(input_data), new Path(input_data_dir));

        // 如果存放 MR 结果的目录已存在，则删除该目录
        if(fileSystem.exists(new Path(output_dir))) {
            fileSystem.delete(new Path(output_dir), true);
        }

        // 关闭文件系统
        fileSystem.close();
    }

    public static void downloadFromHDFS(String hdfs_dir, String local_dir) throws IOException {
        // 从 HDFS 下载文件
        // String local_dir = "/home/student5/lixinli/week02/mr_output_data";
        // String hdfs_dir = local_dir;
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://emr-header-1.cluster-285604:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.copyToLocalFile(new Path(hdfs_dir), new Path(local_dir));
        fileSystem.close();
    }
}
