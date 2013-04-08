package com.senseidb.test;


import com.sensei.search.req.protobuf.SenseiProtos;
import com.senseidb.search.req.*;
import junit.framework.TestCase;

public class TestSerialization extends TestCase {
  public void test() {
    SenseiSnappyProtoSerializer serializer = new SenseiSnappyProtoSerializer();

    SenseiRequest request = new SenseiRequest();
    request.setRouteParam("1");

    SenseiRequest serializedRequest = serializer.requestFromBytes(serializer.requestToBytes(request));
    assertEquals(request, serializedRequest);

    SenseiResult result = new SenseiResult();
    SenseiHit[] hits = new SenseiHit[1];
    hits[0] = new SenseiHit();
    hits[0].setUID(5);

    SenseiHit[] groupHits = new SenseiHit[2];
    groupHits[0] = new SenseiHit();
    groupHits[1] = new SenseiHit();
    hits[0].setGroupHits(groupHits);

//    hits[1] = new SenseiHit();
//    hits[1].setUID(6);


    result.setHits(hits);

    SenseiResult serializedResult = serializer.responseFromBytes(serializer.responseToBytes(result));
    assertEquals(result, serializedResult);
  }
}
