package org.apache.lucene.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Map.Entry;

public class RAMDirectorySerializer {
	
  private static void streamToBytes(RAMFile ramFile,DataOutput out) throws IOException{
	out.writeLong(ramFile.getLastModified());
	out.writeLong(ramFile.sizeInBytes);
	out.writeLong(ramFile.length);
	if (ramFile.buffers==null){
	  out.writeInt(0);
	}
	else{
	  out.writeInt(ramFile.buffers.size());
	  for (byte[] bytes : ramFile.buffers){
	    out.writeInt(bytes.length);
	    out.write(bytes);
	  }
	}
  }
  
  private static void loadFromBytes(RAMFile rfile,DataInput din) throws IOException{
	rfile.setLastModified(din.readLong());
	rfile.sizeInBytes=din.readLong();
	rfile.length = din.readLong();
	
	int count = din.readInt();
	rfile.buffers = new ArrayList<byte[]>(count);
	for (int i=0;i<count;++i){
	  int size = din.readInt();
	  byte[] bytes = new byte[size];
	  din.readFully(bytes);
	  rfile.buffers.add(bytes);
	}
  }
  
  public static byte[] toBytes(RAMDirectory dir) throws IOException{
  //ByteArrayOutputStream bout = new ByteArrayOutputStream((int)dir.sizeInBytes.get());
	ByteArrayOutputStream bout = new ByteArrayOutputStream(dir.sizeInBytes.intValue());
	DataOutputStream dout = new DataOutputStream(bout);
	toDataOutput(dout, dir);
	dout.flush();
	return bout.toByteArray();
  }
  
  public static void toDataOutput(DataOutput dout, RAMDirectory dir) throws IOException{
	dout.writeLong(dir.sizeInBytes());
	dout.writeInt(dir.fileMap.size());
	for (Entry<String,RAMFile> entry : dir.fileMap.entrySet()){
	  dout.writeUTF(entry.getKey());
	  streamToBytes(entry.getValue(),dout);
	}
  }
  
  
  public static RAMDirectory fromBytes(byte[] bytes) throws IOException{
	ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
	DataInputStream din = new DataInputStream(bin);
	return fromDataInput(din);
  }
  
  public static RAMDirectory fromDataInput(DataInput din) throws IOException{
	RAMDirectory rdir = new RAMDirectory();
	rdir.sizeInBytes.set(din.readLong());
	int count = din.readInt();
	for (int i=0;i<count;++i){
	  String fileName = din.readUTF();
	  RAMFile rfile = new RAMFile(rdir);
	  loadFromBytes(rfile, din);
	  rdir.fileMap.put(fileName, rfile);
	}
	return rdir;
  }
  
  private static RAMDirectory createMock(int nfiles,int size) throws IOException{
	RAMDirectory dir = new RAMDirectory();
	for (int i =0;i<nfiles;++i){
	  IndexOutput output = dir.createOutput(String.valueOf(i));
	  byte[] bytes = new byte[size];
	  output.writeBytes(bytes, size);
	  output.flush();
	  output.close();
	}
	return dir;
  }
  
  private static byte[] serializeWithJava(RAMDirectory rdir) throws IOException{
	ByteArrayOutputStream bout = new ByteArrayOutputStream();
	ObjectOutputStream oout = new ObjectOutputStream(bout);
	oout.writeObject(rdir);
	oout.flush();
	return bout.toByteArray();
  }
  
  private static RAMDirectory deserializeWithJava(byte[] bytes) throws Exception{
	ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
	ObjectInputStream oin = new ObjectInputStream(bin);
	return (RAMDirectory)(oin.readObject());
  }
  
  public static void main(String[] args) throws Exception{
	int numFiles = 1;
	int avgSize = 1024;
	int numIter = 20;
	
	long start,end;
	long t1,t2;
	RAMDirectory mockDir = createMock(numFiles, avgSize);
	for (int i=0;i<numIter;++i){
	  start = System.nanoTime();
	  byte[] javaBytes = serializeWithJava(mockDir);
	  end  = System.nanoTime();
	  
	  t1 = (end-start);
	  System.out.println("java ser, size: "+javaBytes.length+", time: "+t1);

	  start = System.nanoTime();
	  byte[] bytes = toBytes(mockDir);
	  end  = System.nanoTime();
	  
	  t2 = (end-start);
	  System.out.println("ser, size: "+bytes.length+", time: "+t2);
	  
	  System.out.println("diff, size: "+(double)bytes.length/(double)javaBytes.length*100.0+", time: "+(double)t2/(double)t1*100.0);
	  
	  start = System.nanoTime();
	  mockDir = deserializeWithJava(javaBytes);
	  end  = System.nanoTime();
	  t1=end-start;
	  System.out.println("java deser, time: "+t1);

	  start = System.nanoTime();
	  mockDir = fromBytes(bytes);
	  end  = System.nanoTime();
	  t2 = end-start;
	  System.out.println("deser, time: "+t2);
	  
	  System.out.println("diff, time: "+(double)t2/(double)t1*100.0);
	}
  }
}
