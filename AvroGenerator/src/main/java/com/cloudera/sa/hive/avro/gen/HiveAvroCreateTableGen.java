package com.cloudera.sa.hive.avro.gen;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.data.Json;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

public class HiveAvroCreateTableGen 
{
	DataFileWriter d;
	
    public static void main( String[] args ) throws IOException
    {
        if (args.length != 2) {
        	System.out.println("HiveAvroCreateTableGen");
        	System.out.println("Parameters: <hdfs file path> <local hive create script file path>");
        	
        	return;
        }
        
        String inputFile = args[0];
        String outputFile = args[1];
		
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		
		Path inputFilePath = new Path(inputFile);

		FSDataInputStream dataInputStream = hdfs.open(inputFilePath);
		
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
		//writer.setSchema(s); // I guess I don't need this
		
		DataFileStream<GenericRecord> dataFileReader = new DataFileStream<GenericRecord>(dataInputStream, reader);
		try {
			Schema s = dataFileReader.getSchema();
			
			System.out.println("-------------------");
			System.out.println("Avro Schema:");
			System.out.println("-------------------");
			System.out.println(s.getName() + " " + s);
			
			String hiveScript = convertAvroSchemaToGlueSchema(s);
			
			System.out.println("-------------------");
			System.out.println("Create table script:");
			System.out.println("-------------------");
			System.out.println(hiveScript);
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));
			try {
				writer.write(hiveScript);
			} finally {
				writer.close();	
			}
			
		} finally {
			dataFileReader.close();
		}
	    
    }

    public static Map<String, String> convertAvroSchemaToAthenaDatatypes(Schema s){
		Map<Schema.Type, String> avroToHiveType = new HashMap<Schema.Type, String>();
		avroToHiveType.put(Schema.Type.STRING, "string");
		avroToHiveType.put(Schema.Type.BYTES, "binary");
		avroToHiveType.put(Schema.Type.INT, "int");
		avroToHiveType.put(Schema.Type.LONG, "bigint");
		avroToHiveType.put(Schema.Type.FLOAT, "float");
		avroToHiveType.put(Schema.Type.DOUBLE, "double");
		avroToHiveType.put(Schema.Type.BOOLEAN, "boolean");

		Map<String, String> out = new HashMap<String, String>();
		for(Schema.Field f:s.getFields()){
			System.out.println("Name: " + f.name());
			System.out.println("Schema: " + f.schema());
			System.out.println("Type: " + f.schema().getType());

			Schema.Type type = f.schema().getType();
			if(type.equals(Schema.Type.UNION)){
				List<Schema> allTypes = f.schema().getTypes();
				for(Schema t: allTypes){
					if(!t.getType().equals(Schema.Type.NULL)){
						type = t.getType();
					}
				}
			}
			String athenaType = avroToHiveType.get(type);
			if(athenaType == null){
				throw new Error("Could not find type: " + type + " for column: " + f.name());
			}
			out.put(f.name(), athenaType);
		}

    	return out;
	}

	public static String convertAvroSchemaToAthenaSchema(Schema s){
		String lineSeparator = System.getProperty("line.separator");
		Map<String, String> datatypes = HiveAvroCreateTableGen.convertAvroSchemaToAthenaDatatypes(s);

    	String columnDefs = "( " + lineSeparator;
    	int i = 0;
    	for(Map.Entry<String,String> kvp: datatypes.entrySet()){
    		i++;
    		String maybeComma = "";
    		if(i != datatypes.size()){
    			maybeComma = " , ";
			}
    		columnDefs += kvp.getKey() + " " + kvp.getValue() + maybeComma + lineSeparator;
		}
    	columnDefs += " )\n";
    	String DDL = "CREATE EXTERNAL TABLE " + s.getName() + columnDefs;
    	DDL += "--PARTITIONED BY (date string)" + lineSeparator;
    	DDL += "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'" + lineSeparator;
    	DDL += "WITH SERDEPROPERTIES ('avro.schema.literal'=' " + s.toString() + "')" + lineSeparator;
    	DDL += "STORED AS AVRO" + lineSeparator;
    	DDL += "--LOCATION 's3://'";
    	return DDL;
	}

	private static ObjectNode convertMapEntryToJson(Map.Entry<String, String> entry){
    	ObjectNode obj = JsonNodeFactory.instance.objectNode();
    	obj.put("Name", entry.getKey());
    	obj.put("Type", entry.getValue());
    	return obj;
	}

	public static String convertAvroSchemaToGlueSchema(Schema s){
		Map<String, String> datatypes = HiveAvroCreateTableGen.convertAvroSchemaToAthenaDatatypes(s);

		ArrayNode columns = JsonNodeFactory.instance.arrayNode();
		for(Map.Entry<String, String> kvp: datatypes.entrySet()){
			columns.add(HiveAvroCreateTableGen.convertMapEntryToJson(kvp));
		}

		ObjectNode tableInput = JsonNodeFactory.instance.objectNode();

		ObjectNode params = JsonNodeFactory.instance.objectNode();
		params.put("EXTERNAL", "TRUE");

		ObjectNode serdeInfo = JsonNodeFactory.instance.objectNode();
		ObjectNode serdeParams = JsonNodeFactory.instance.objectNode();
		serdeParams.put("serialization.format", "1");

		try{
			final ObjectMapper mapper = new ObjectMapper();
			JsonNode avroSchema = mapper.readTree(s.toString());
			serdeParams.put("avro.schema.literal", avroSchema);
		}catch (Exception e){
			throw new RuntimeException(e);
		}


		serdeInfo.put("SerializationLibrary",  "org.apache.hadoop.hive.serde2.avro.AvroSerDe");
		serdeInfo.put("Parameters", serdeParams);

		ObjectNode storageDescriptor = JsonNodeFactory.instance.objectNode();
		storageDescriptor.put("InputFormat",  "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
		storageDescriptor.put("OutputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
		storageDescriptor.put("Location", "location");
		storageDescriptor.put("Columns", columns);
		storageDescriptor.put("SerdeInfo", serdeInfo);


		ArrayNode partitionKeys = JsonNodeFactory.instance.arrayNode();
		ObjectNode datePartition = JsonNodeFactory.instance.objectNode();
		datePartition.put("Name", "date");
		datePartition.put("Type", "string");
		partitionKeys.add(datePartition);

		tableInput.put("Name", "table_name");
		tableInput.put("TableType", "EXTERNAL_TABLE");
		tableInput.put("Parameters", params);
		tableInput.put("StorageDescriptor", storageDescriptor );
		tableInput.put("PartitionKeys", partitionKeys);


		ObjectNode root = JsonNodeFactory.instance.objectNode();
		root.put("DatabaseName", "db_name");
		root.put("TableInput", tableInput);

		return root.toString();
	}
    
    public static String generateCreateTableScript(Schema s) {
    	String lineSeparator = System.getProperty("line.separator");
    	
    	StringBuilder strBuilder = new StringBuilder();
    	
    	strBuilder.append("CREATE TABLE " + s.getName() + lineSeparator);
    	strBuilder.append("ROW FORMAT " + lineSeparator + 
                          "SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' " + lineSeparator + 
                          "STORED AS " + lineSeparator + 
                          "INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' " + lineSeparator + 
                          "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' " + lineSeparator + 
                          "TBLPROPERTIES ('avro.schema.literal'=' " + lineSeparator + s.toString() + "');");
    	
    	return strBuilder.toString();
    }
}
