/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.server.UID;

/**
 * Wrapper around an Avro GenericRecord.  Necessary because Hive's deserializer
 * will happily deserialize any object - as long as it's a writable.
 */
public class AvroSpecificRecordWritable<T extends SpecificRecord> implements Writable{
  T record;
  private BinaryDecoder binaryDecoder;

  // Schema that exists in the Avro data file.
  private Schema fileSchema;

  /**
   * Unique Id determine which record reader created this record
   */
  private UID recordReaderID;

  // There are two areas of exploration for optimization here.
  // 1.  We're serializing the schema with every object.  If we assume the schema
  //     provided by the table is always correct, we don't need to do this and
  //     and can just send the serialized bytes.
  // 2.  We serialize/deserialize to/from bytes immediately.  We may save some
  //     time but doing this lazily, but until there's evidence this is useful,
  //     it's not worth adding the extra state.
  public SpecificRecord getRecord() {
    return record;
  }

  public void setRecord(T record) {
    this.record = record;
  }

  public AvroSpecificRecordWritable() {}

  public AvroSpecificRecordWritable(T record) {
    this.record = record;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // Write schema since we need it to pull the data out. (see point #1 above)
    String schemaString = record.getSchema().toString(false);
    out.writeUTF(schemaString);

    schemaString = fileSchema.toString(false);
    out.writeUTF(schemaString);

    recordReaderID.write(out);

    // Write record to byte buffer
    SpecificDatumWriter<T> sdw = new SpecificDatumWriter<T>(record.getSchema());
    BinaryEncoder be = EncoderFactory.get().directBinaryEncoder((DataOutputStream)out, null);

    sdw.write(record, be);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Schema schema = AvroSerdeUtils.getSchemaFor(in.readUTF());
    fileSchema = AvroSerdeUtils.getSchemaFor(in.readUTF());
    recordReaderID = UID.read(in);

    binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder((InputStream) in, binaryDecoder);
    DatumReader<T> datumReader = new SpecificDatumReader(schema);
    record = datumReader.read(null, binaryDecoder);
  }
  
  public void readFields(byte[] bytes, int offset, int length, Schema writerSchema, Schema readerSchema) throws IOException {
    fileSchema = writerSchema;
    binaryDecoder =
        DecoderFactory.get().binaryDecoder(bytes, offset, length - offset,
	          binaryDecoder);
    DatumReader<T> datumReader = new SpecificDatumReader(readerSchema,writerSchema);

    record = datumReader.read(null, binaryDecoder);
  }

  public void readFields(byte[] bytes, Schema writerSchema, Schema readerSchema) throws IOException {
    fileSchema = writerSchema;
    DatumReader<T> datumReader = new SpecificDatumReader(readerSchema,writerSchema);
    ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    DataFileStream<T> dfr = new DataFileStream(is, datumReader);

    record = dfr.next();
    dfr.close();
  }

  public UID getRecordReaderID() {
    return recordReaderID;
  }

  public void setRecordReaderID(UID recordReaderID) {
    this.recordReaderID = recordReaderID;
  }

  public Schema getFileSchema() {
    return fileSchema;
  }

  public void setFileSchema(Schema originalSchema) {
    this.fileSchema = originalSchema;
  }
}
