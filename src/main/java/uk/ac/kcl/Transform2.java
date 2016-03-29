/* 
 * Copyright 2016 King's College London, Richard Jackson <richgjackson@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.kcl;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import java.io.ByteArrayInputStream;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class Transform2 {

    public static class SomeMapper extends Mapper {      
        
     @Override
        protected void map(Object key, Object value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            //check if line starts with MSH
            // if(line.startsWith(MSH_SEG_START))
            //File file = new File("hl7_messages.txt");
            InputStream is = new ByteArrayInputStream(line.getBytes(StandardCharsets.UTF_8));
            // It's generally a good idea to buffer file IO

            // The following class is a HAPI utility that will iterate over
            // the messages which appear over an InputStream
            Hl7InputStreamMessageIterator iter = new Hl7InputStreamMessageIterator(is);
            while (iter.hasNext()) {
                Message next = iter.next();
                String json = Transform2.convertHL7ToJson(next);
                System.out.println();
                BytesWritable jsonDoc = new BytesWritable(json.getBytes());
                // send the doc directly
                context.write(NullWritable.get(), jsonDoc);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "127.0.0.1:9200");
        conf.set("es.resource", "hl7/message");
        conf.set("es.input.json", "yes");
//        conf.set("es.net.ssl", "true");
//        conf.set("es.net.ssl.keystore.location","/home/rich/junk/node01.jks");
//        conf.set("es.net.ssl.keystore.pass","");
//        conf.set("es.net.ssl.truststore.location", "/home/rich/junk/node01.jks");
//        conf.set("es.net.ssl.truststore.pass", "");        
//        conf.set("es.net.http.auth.user", "admin");
//        conf.set("es.net.http.auth.pass", "");

        Job job = Job.getInstance(conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SomeMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.waitForCompletion(true);
    }

    public static String convertHL7ToJson(Message message) {

        try {
            DefaultXMLParser xmlParser = new DefaultXMLParser(new CanonicalModelClassFactory("2.4"));
            String xml = xmlParser.encode(message);
            XmlMapper xmlMapper = new XmlMapper();
            System.out.println(xml);
            List entries = null;
            try {
                entries = xmlMapper.readValue(xml, List.class);
            } catch (IOException ex) {
                Logger.getLogger(Transform2.class.getName()).log(Level.SEVERE, null, ex);
            }

            ObjectMapper jsonMapper = new ObjectMapper();
            
            String json = null;
            try {
                json = jsonMapper.writeValueAsString(entries);
            } catch (IOException ex) {
                Logger.getLogger(Transform2.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            System.out.println(json);
            json = json.substring(1, (json.length()-1));
            //to do  - add code to rename fields, removing periods
            
 
            return json;
        } catch (HL7Exception ex) {
            Logger.getLogger(Transform2.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
