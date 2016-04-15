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
import ca.uhn.hl7v2.model.AbstractMessage;
import ca.uhn.hl7v2.model.DataTypeException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.model.v231.datatype.TS;
import ca.uhn.hl7v2.model.v231.segment.MSH;

import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
import ca.uhn.hl7v2.parser.DefaultXMLParser;
import ca.uhn.hl7v2.util.Hl7InputStreamMessageIterator;
import ca.uhn.hl7v2.util.Terser;
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
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Transform231 {

    public static class MapToHL7String extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context)
                throws IOException, InterruptedException {
            // create the MapWritable object
            MapWritable doc = new MapWritable();
            doc.putAll(Transform231.extractHL7Metadata(value.toString()));
            if(doc.size()!=0){
                context.write(NullWritable.get(), doc);
            }
        }
    }

    public static Map<Text, Writable> extractHL7Metadata(String messageString) {
        InputStream is = new ByteArrayInputStream(messageString.getBytes(StandardCharsets.UTF_8));
        Hl7InputStreamMessageIterator iter = new Hl7InputStreamMessageIterator(is);
        Map<Text, Writable> map = new HashMap<>();
        while (iter.hasNext()) {
            Message next = iter.next();
            Text fieldName = new Text("body");
            try {
                if (!next.printStructure().startsWith("ACK") ) {
                    try {
                        map.put(fieldName, new Text(next.printStructure()));
                    } catch (HL7Exception ex) {
                        Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    try {
                        map.put(new Text("message_time_stamp"), Transform231.getIso8601TimeFromMSH(Transform231.getMSH((AbstractMessage) next)));
                    } catch (HL7Exception ex) {
                        Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                    try {
                        map.put(new Text("pid"), Transform231.getPID((AbstractMessage) next));
                    } catch (HL7Exception ex) {
                        Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } catch (HL7Exception ex) {
                Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //example config
//        conf.set("es.nodes", "192.168.1.101:9200");
//        conf.set("es.resource", "hl7/message");
//        conf.set("es.input.json", "yes");
//        conf.set("es.net.ssl", "true");
//        conf.set("es.net.ssl.keystore.location", "file:///home/rich/junk/node01.jks");
//        conf.set("es.net.ssl.keystore.pass", "");
//        conf.set("es.net.ssl.truststore.location", "file:///home/rich/junk/node01.jks");
//        conf.set("es.net.ssl.truststore.pass", "");
//        conf.set("es.net.http.auth.user", "admin");
//        conf.set("es.net.http.auth.pass", "");
        conf.set("es.nodes", args[2]);
        conf.set("es.resource", args[10] + args[11]);
        conf.set("textinputformat.record.delimiter", "\n");
        //conf.set("es.input.json", "yes");
        if (args[3].equalsIgnoreCase("true")) {
            conf.set("es.net.ssl", "true");
            conf.set("es.net.ssl.keystore.location", args[4]);
            conf.set("es.net.ssl.keystore.pass", args[5]);
            conf.set("es.net.ssl.truststore.location", args[6]);
            conf.set("es.net.ssl.truststore.pass", args[7]);
            conf.set("es.net.http.auth.user", args[8]);
            conf.set("es.net.http.auth.pass", args[9]);
        }
        Job job = Job.getInstance(conf);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapToHL7String.class);
        job.setMapOutputKeyClass(NullWritable.class);
        //change if using re-existing json
        //job.setMapOutputValueClass(BytesWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.waitForCompletion(true);
    }

    public static String convertHL7ToJson(Message message) {
        try {
            DefaultXMLParser xmlParser = new DefaultXMLParser(new CanonicalModelClassFactory("2.4"));
            Document xml = xmlParser.encodeDocument(message);
            cleanFieldNames(xml.getChildNodes().item(0));
            XmlMapper xmlMapper = new XmlMapper();
            List entries = null;
            try {
                entries = xmlMapper.readValue(getStringFromDocument(xml), List.class);
            } catch (IOException | TransformerException ex) {
                Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
            }

            ObjectMapper jsonMapper = new ObjectMapper();

            String json = null;
            try {
                json = jsonMapper.writeValueAsString(entries);
            } catch (IOException ex) {
                Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
            }

            //System.out.println(json);
            json = json.substring(1, (json.length() - 1));
            //to do  - add code to rename fields, removing periods

            return json;
        } catch (HL7Exception ex) {
            Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    public static String getStringFromDocument(Document doc) throws TransformerException {
        DOMSource domSource = new DOMSource(doc);
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.transform(domSource, result);
        return writer.toString();
    }

    public static void cleanFieldNames(Node node) {
        // do something with the current node instead of System.out
        //System.out.println(node.getNodeName());

        NodeList nodeList = node.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node currentNode = nodeList.item(i);
            if (currentNode.getNodeType() == Node.ELEMENT_NODE) {
                //calls this method for all the children which is Element
                Element el = (Element) nodeList.item(i);
                //System.out.println(el.getNodeName());
                //System.out.println(nodeList.getLength());
                if (el.getNodeName().contains(".")) {
                    el.getOwnerDocument().renameNode(nodeList.item(i), null, el.getNodeName().replace(".", "_"));
                }
                cleanFieldNames(currentNode);
            }
        }
    }

    public static Writable getIso8601TimeFromMSH(MSH msh) {

        TS ts = msh.getDateTimeOfMessage();
        Date date = null;
        try {
            date = ts.getTimeOfAnEvent().getValueAsDate();
        } catch (DataTypeException ex) {
            Logger.getLogger(Transform231.class.getName()).log(Level.SEVERE, null, ex);
            return NullWritable.get();
        }

        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
        String returnString = null;
        try {
            returnString = df.format(date);
        } catch (NullPointerException ex) {
            return NullWritable.get();
        }
        System.out.println(returnString);
        return new Text(returnString);
    }

    public static MSH getMSH(AbstractMessage message) throws HL7Exception {
        return (MSH) message.get("MSH");
    }

    public static Writable getPID(AbstractMessage message) throws HL7Exception {
        Terser terser = new Terser(message);
        Text t;
            t = new Text(terser.get("/.PID-3-1"));    
        if (t != null) {
            return t;
        } else {
            return NullWritable.get();
        }
    }
}
