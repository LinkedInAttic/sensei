package com.sensei.test;

import java.util.Date;

import junit.framework.TestCase;

import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Field.TermVector;

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
		
		@Text(name="text")
		private String content;
		
		@Text(name="text",store=Store.YES,index=Index.ANALYZED,termVector=TermVector.WITH_POSITIONS_OFFSETS)
		private String content2;
		
		@Meta
		private int age=5;
		
		@Meta(name="birthday",type=MetaType.Date)
		private String bday="";
		

		@Meta(format="yyyyMMdd",type=MetaType.Date)
		private Date today;
		
		@Meta(type=MetaType.String)
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
	
	private TestObj dataObj = new TestObj();
	
	public SenseiIndexingAPITest() {
	}

	public SenseiIndexingAPITest(String name) {
		super(name);
	}
	
	private void initData(){
		dataObj.uid = 1;
		dataObj.content="abc";
		dataObj.content2="def";
	}

	public static void main(String[] args) {
		DefaultSenseiInterpreter<TestObj> nodeInterpreter = new DefaultSenseiInterpreter(TestObj.class);
		System.out.println(nodeInterpreter);
	}
}
