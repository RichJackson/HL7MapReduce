/*
Author: Jayant Singh
Website: http://www.j4jayant.com
Description:
This Hadoop MapReduce code extends wordcount example & counts different trigger events(MSH_9) present in HL7 file
 */
package uk.ac.kcl;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v24.datatype.PN;
import ca.uhn.hl7v2.model.v24.datatype.XPN;
import ca.uhn.hl7v2.model.v24.message.ADT_A01;
import ca.uhn.hl7v2.model.v24.message.ORU_R01;
import ca.uhn.hl7v2.model.v24.segment.MSH;
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import java.io.ByteArrayInputStream;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

public class Transform {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        final String MSH_SEG_START = "MSH|^~\\&";

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            //check if line starts with MSH
            // if(line.startsWith(MSH_SEG_START))
            //File file = new File("hl7_messages.txt");
            InputStream is = new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));
            // It's generally a good idea to buffer file IO

            // The following class is a HAPI utility that will iterate over
            // the messages which appear over an InputStream
            Hl7InputStreamMessageIterator iter = new Hl7InputStreamMessageIterator(is);
            int i = 0;
            while (iter.hasNext()) {
                Message next = iter.next();                
                System.out.println(convertHL7ToJson(next));

            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(EventCount.class);
        conf.setJobName("EventCount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

   static String convertHL7ToJson(Message message) {

        try {
            DefaultXMLParser xmlParser = new DefaultXMLParser(new CanonicalModelClassFactory("2.4"));
            String xml = xmlParser.encode(message);
            XmlMapper xmlMapper = new XmlMapper();
            Map entries = null;
            try {
                entries = xmlMapper.readValue(xml, Map.class);
            } catch (IOException ex) {
                Logger.getLogger(Transform.class.getName()).log(Level.SEVERE, null, ex);
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            String json = null;
            try {
                json = jsonMapper.writeValueAsString(entries);
            } catch (IOException ex) {
                Logger.getLogger(Transform.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println(json);
            return xml;
        } catch (HL7Exception ex) {
            Logger.getLogger(Transform.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}