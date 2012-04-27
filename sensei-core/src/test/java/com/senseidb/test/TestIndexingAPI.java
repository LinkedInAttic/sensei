package com.senseidb.test;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

import com.linkedin.zoie.api.indexing.ZoieIndexable;
import com.linkedin.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.senseidb.indexing.DefaultSenseiInterpreter;
import com.senseidb.indexing.DeleteChecker;
import com.senseidb.indexing.Meta;
import com.senseidb.indexing.MetaType;
import com.senseidb.indexing.SkipChecker;
import com.senseidb.indexing.StoredValue;
import com.senseidb.indexing.Text;
import com.senseidb.indexing.Uid;

public class TestIndexingAPI extends TestCase {

	static class TestObj{
		@Uid
		private long uid;
		
		TestObj(long uid){
		  this.uid = uid;
		}
		
		@Text(name="text")
		private String content;
		
		@Text(store="YES",index="NOT_ANALYZED",termVector="WITH_POSITIONS_OFFSETS")
		private String content2;
		
		@StoredValue(name="store")
		private String storedVal;
		
		@Meta
		private int age;

		@Meta(format="yyyyMMdd",type=MetaType.Date)
		private Date today;
		
		@Meta(name="shortie",type=MetaType.String)
		private short shortVal;
		
		@Meta
		private List<String> tags;
		
		@Meta(name="nulls",type=MetaType.Long)
		private List<Long> nulls;
		
		
		@Meta(name="numbers",type=MetaType.Integer)
		private Set<Integer> numSet;
		
		@DeleteChecker
		private boolean isDeleted(){
			return uid==-1;
		}
		
		@SkipChecker
		private boolean isSkip(){
			return uid==-2;
		}
	}
	

	private DefaultSenseiInterpreter<TestObj> nodeInterpreter = new DefaultSenseiInterpreter(TestObj.class);
	
	
	public TestIndexingAPI() {
	}

	public TestIndexingAPI(String name) {
		super(name);
	}
	
	public void testDelete(){
		TestObj testObj = new TestObj(5);
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		assertFalse(indexable.isDeleted());
		
		testObj = new TestObj(-1);
		indexable = nodeInterpreter.convertAndInterpret(testObj);
		assertTrue(indexable.isDeleted());
	}
	
	public void testSkip(){
		TestObj testObj = new TestObj(-1);
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		assertFalse(indexable.isSkip());
		
		testObj = new TestObj(-2);
		indexable = nodeInterpreter.convertAndInterpret(testObj);
		assertTrue(indexable.isSkip());
	}
	
	public void testUid(){
		long uid = 13;
		TestObj testObj = new TestObj(uid);
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		assertEquals(13,indexable.getUID());
	}
	
	public void testStoredContent(){
	   TestObj testObj = new TestObj(1);
	   testObj.storedVal = "stored";
	   
	   ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		IndexingReq[] reqs = indexable.buildIndexingReqs();
		Document doc = reqs[0].getDocument();
		Field f = doc.getField("store");
		assertEquals("stored",f.stringValue());
		assertTrue(f.isStored());
		assertFalse(f.isTermVectorStored());
		assertFalse(f.isIndexed());
		assertFalse(f.isTokenized());
	}
	
	public void testTextContent(){
		TestObj testObj = new TestObj(1);
		testObj.content = "abc";
		testObj.content2 = "def";
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		IndexingReq[] reqs = indexable.buildIndexingReqs();
		Document doc = reqs[0].getDocument();
		Field content1Field = doc.getField("text");
		assertNotNull(content1Field);
		String val = content1Field.stringValue();
		assertEquals("abc",val);
		assertFalse(content1Field.isStored());
		assertFalse(content1Field.isTermVectorStored());
		assertTrue(content1Field.isIndexed());
		assertFalse(content1Field.isTokenized());
		
		Field content2Field = doc.getField("content2");
		assertNotNull(content2Field);
		val = content2Field.stringValue();
		assertEquals("def",val);
		assertTrue(content2Field.isStored());
		assertTrue(content2Field.isTermVectorStored());
		assertTrue(content2Field.isIndexed());
		assertFalse(content2Field.isTokenized());
	}
	
	private static boolean isMeta(Field f){
		return !f.isStored() && f.isIndexed() && !f.isTermVectorStored() && !f.isTokenized();
	}
	
	public void testMetaContent(){
		long now = System.currentTimeMillis();
		TestObj testObj = new TestObj(1);
		testObj.age = 11;
		testObj.shortVal=3;
		testObj.today = new Date(now);
		testObj.tags = new ArrayList<String>();
		testObj.tags.add("t1");
		testObj.tags.add("t2");
		testObj.numSet = new HashSet<Integer>();
		testObj.numSet.add(13);
		testObj.numSet.add(6);
		testObj.numSet.add(7);
		
		testObj.nulls = null;
		
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		IndexingReq[] reqs = indexable.buildIndexingReqs();
		Document doc = reqs[0].getDocument();
		
		Field ageField = doc.getField("age");
		assertNotNull(ageField);
		assertTrue(isMeta(ageField));
		String ageString = ageField.stringValue();
		String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Integer);
		Format formatter = new DecimalFormat(formatString, new DecimalFormatSymbols(Locale.US));
		assertEquals(formatter.format(11),ageString);
		
		Field shortField = doc.getField("shortie");
		assertNotNull(shortField);
		assertTrue(isMeta(shortField));
		String shortString = shortField.stringValue();
		assertEquals("3",shortString);
		
		Field todayField = doc.getField("today");
		assertNotNull(todayField);
		assertTrue(isMeta(todayField));
		String todayString = todayField.stringValue();
		formatString = "yyyyMMdd";
		formatter = new SimpleDateFormat(formatString);
		assertEquals(todayString,formatter.format(testObj.today));
		
		Field[] fields = doc.getFields("tags");
		assertEquals(2, fields.length);
		for (Field f : fields){
		  assertTrue(testObj.tags.contains(f.stringValue()));
		}
		
		fields = doc.getFields("numbers");
		assertEquals(3, fields.length);
		for (Field f : fields){
		  assertTrue(testObj.numSet.contains(Integer.parseInt(f.stringValue())));
		}
	}
	
	public static void main(String[] args) {
		DefaultSenseiInterpreter<TestObj> nodeInterpreter = new DefaultSenseiInterpreter(TestObj.class);
		System.out.println(nodeInterpreter);
	}
}
