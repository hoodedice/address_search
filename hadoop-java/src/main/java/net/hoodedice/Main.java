package net.hoodedice;

import org.apache.avro.data.Json;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * https://gist.github.com/nalvared/df3f92e9268a0799eeb6333cc5dc3003
 */
public class Main extends Configured implements Tool {
    static final String inputFile = "hdfs:///user/root/full_address_data.csv";
    static final String outputFile = "hdfs:///user/root/test.csv";

    public static final Logger log = Logger.getLogger(Main.class);

//    static final String outputFile = "addresses.json";

    public static void main(String[] args) throws Exception {
        log.setLevel(Level.WARN);

        try {
            int res = ToolRunner.run(new Configuration(), new Main(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }

        /*
 Set the output key and value classes

*/

    }

    public int run(String[] args) throws Exception {
        String query = args[0];

        System.out.println("Got arg :" + args[0]);

        if (query == null || query.isEmpty()) {
            throw new Error("A query is required");
        }

        Configuration conf = new Configuration();
        conf.set("query", query);
        conf.set("mapreduce.map.java.opts", "-Xmx4096m"); // Adjust the heap size as needed
//        conf.set("mapreduce.reduce.java.opts", "-Xmx10737m"); // Adjust the heap size as needed


        // https://stackoverflow.com/a/31823374
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputFile))) hdfs.delete(new Path(outputFile), true);

        Job job = Job.getInstance(conf, "Address Search");
        job.setJarByClass(Main.class);
        job.setJobName("Address Search");

        // Set the mapper and reducer classes
        job.setMapperClass(AddressMapper.class);
        job.setReducerClass(AddressReducer.class);
//        job.setNumReduceTasks(0);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        // Set the input and output paths
        FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        System.out.println("Hadoop configuration finished");

        job.waitForCompletion(true);

        return 0;
    }


    public static class AddressMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final static LongWritable id = new LongWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String query = conf.get("query");
            String[] row = value.toString().split(",");
            boolean match = false;
//            System.out.println("AddressMapper Map: " + value);
//            System.out.println("AddressMapper Map id: " + row[0]);

            //skip the header row
            if (!Objects.equals(row[0], "id")) {
                try {

                    // key
                    id.set(Long.parseLong(row[0]));

                    // search columns:
                    String number = row[2];
                    String street = row[3];
                    String street2 = row[4];

                    if (number.contains(query) || street.contains(query) || street2.contains(query)) {
                        context.write(id, value);
                    }
                } catch (Exception e) {
                    System.out.println("AddressMapper Map: (error row)" + value);
                }
            }
        }
    }

    public static class AddressReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
        private Text outputValue = new Text();
        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Output as is (csv)
            // for (Text value : values) {
            //     context.write(key, value);
            // }
            List<Object> jsonArray = new ArrayList<>();

            for (Text value: values) {
                String[] row = value.toString().split(",");

                if (row.length == 9) {

                    // Long should be string for json
                    String id = row[0];
                    int zip = Integer.parseInt(row[1]);
                    String number = row[2];
                    String street = row[3];
                    String street2 = row[4];
                    String city = row[5];
                    String state = row[6];
                    Double latitude = Double.valueOf(row[7]);
                    Double longitude = Double.valueOf(row[8]);

                    Object jsonObject = new ObjectMapper().createObjectNode()
                            .put("id", id)
                            .put("zip", zip)
                            .put("number", number)
                            .put("street", street)
                            .put("street2", street2)
                            .put("city", city)
                            .put("state", state)
                            .put("latitude", latitude)
                            .put("longitude", longitude);

                    jsonArray.add(jsonObject);
                }
            }

            // Convert the List of Maps to a JSON array
            String jsonArrayString = objectMapper.writeValueAsString(jsonArray);

            // Output the JSON array
            outputValue.set(jsonArrayString);
            context.write(NullWritable.get(), outputValue);

        }
    }
}
