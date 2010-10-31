package com.sensei.test;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

import junit.framework.TestCase;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

import com.sensei.indexing.api.DefaultSenseiInterpreter;
import com.sensei.indexing.api.DeleteChecker;
import com.sensei.indexing.api.Meta;
import com.sensei.indexing.api.MetaType;
import com.sensei.indexing.api.SkipChecker;
import com.sensei.indexing.api.Text;
import com.sensei.indexing.api.Uid;

public class SenseiIndexingAPITest extends TestCase {

	static class TestObj{
		@Uid
		private long uid;
		
		TestObj(long uid){
		  this.uid = uid;
		}
		
		@Text(name="text")
		private String content;
		
		@Text(store=Store.YES,index=Index.NOT_ANALYZED,termVector=TermVector.WITH_POSITIONS_OFFSETS)
		private String content2;
		
		@Meta
		private int age;

		@Meta(format="yyyyMMdd",type=MetaType.Date)
		private Date today;
		
		@Meta(name="shortie",type=MetaType.String)
		private short shortVal;
		
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
	
	
	public SenseiIndexingAPITest() {
	}

	public SenseiIndexingAPITest(String name) {
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
		assertTrue(content1Field.isTokenized());
		
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
		
		ZoieIndexable indexable = nodeInterpreter.convertAndInterpret(testObj);
		IndexingReq[] reqs = indexable.buildIndexingReqs();
		Document doc = reqs[0].getDocument();
		
		Field ageField = doc.getField("age");
		assertNotNull(ageField);
		assertTrue(isMeta(ageField));
		String ageString = ageField.stringValue();
		String formatString = DefaultSenseiInterpreter.DEFAULT_FORMAT_STRING_MAP.get(MetaType.Integer);
		Format formatter = new DecimalFormat(formatString);
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
	}
	
	public static void main(String[] args) {
		DefaultSenseiInterpreter<TestObj> nodeInterpreter = new DefaultSenseiInterpreter(TestObj.class);
		System.out.println(nodeInterpreter);
	}
}
