package com.cloudera.sa.hive.avro.gen;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.NullNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }

    public void testConvertAvroSchemaToAthenaDataTypes(){
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(
                new Schema.Field("col_one", Schema.create(Schema.Type.INT), "this is a doc", NullNode.getInstance())
        );
        fields.add(
                new Schema.Field("col_two", Schema.create(Schema.Type.STRING), "this is a doc", NullNode.getInstance())
        );

        List<Schema> unionTypes = new ArrayList<Schema>();
        unionTypes.add(Schema.create(Schema.Type.NULL));
        unionTypes.add(Schema.create(Schema.Type.STRING));

        fields.add(
                new Schema.Field("col_three", Schema.createUnion(unionTypes), "this is a doc", NullNode.getInstance())
        );
        Schema avroSchema = Schema.createRecord(fields);

        Map<String, String> expected = new HashMap<String, String>();
        expected.put("col_one", "int");
        expected.put("col_two", "string");
        expected.put("col_three", "string");

        Map<String, String> out = HiveAvroCreateTableGen.convertAvroSchemaToAthenaDatatypes(avroSchema);
        assertEquals(expected, out);
    }

    public void testConvertAvroSchemaToAthenaDDLString(){
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(
                new Schema.Field("col_one", Schema.create(Schema.Type.INT), "this is a doc", NullNode.getInstance())
        );
        fields.add(
                new Schema.Field("col_two", Schema.create(Schema.Type.STRING), "this is a doc", NullNode.getInstance())
        );
        Schema avroSchema = Schema.createRecord("MyTable", "docstring", "mynamespace", false);
        avroSchema.setFields(fields);
        String expected = "CREATE EXTERNAL TABLE MyTable( \n" +
                "col_one int , \n" +
                "col_two string\n" +
                " )\n" +
                "--PARTITIONED BY (date string)\n" +
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'\n" +
                "WITH SERDEPROPERTIES ('avro.schema.literal'=' {\"type\":\"record\",\"name\":\"MyTable\",\"namespace\":\"mynamespace\",\"doc\":\"docstring\",\"fields\":[{\"name\":\"col_one\",\"type\":\"int\",\"doc\":\"this is a doc\",\"default\":null},{\"name\":\"col_two\",\"type\":\"string\",\"doc\":\"this is a doc\",\"default\":null}]}')\n" +
                "STORED AS AVRO\n" +
                "--LOCATION 's3://'";

        String out = HiveAvroCreateTableGen.convertAvroSchemaToAthenaSchema(avroSchema);
        assertEquals(expected, out);
    }
}
