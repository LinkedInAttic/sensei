package com.senseidb.search.req;


import com.alibaba.fastjson.JSON;
import com.browseengine.bobo.api.*;
import com.browseengine.bobo.facets.DefaultFacetHandlerInitializerParam;
import com.browseengine.bobo.facets.FacetHandlerInitializerParam;
import com.browseengine.bobo.mapred.MapReduceResult;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.linkedin.norbert.network.Serializer;
import com.sensei.search.req.protobuf.SenseiProtos;
import com.senseidb.search.req.mapred.SenseiMapReduce;
import com.senseidb.search.req.mapred.functions.*;
import com.senseidb.util.JSONUtil;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.SortField;
import org.json.JSONException;

import java.io.*;
import java.util.*;


public class SenseiRequestProtoSerializer implements Serializer<SenseiRequest, SenseiResult> {
  private final static Logger logger = Logger.getLogger(SenseiRequestProtoSerializer.class);

  @Override
  public String requestName() {
    return "SenseiRequestV2";
  }

  @Override
  public String responseName() {
    return "SenseiResponseV2";
  }

  @Override
  public SenseiRequest requestFromBytes(byte[] bytes) {
    SenseiProtos.SenseiProtoRequest senseiProtoRequest = null;
    try {
      senseiProtoRequest = SenseiProtos.SenseiProtoRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Could not parse");
    }

    SenseiRequest senseiRequest = new SenseiRequest();

    for (SenseiProtos.BrowseSelection selection : senseiProtoRequest.getSelectionList()) {
      senseiRequest.addSelection(convertBrowseSelection(selection));
    }

    for (SenseiProtos.SortField sortField : senseiProtoRequest.getSortSpecList()) {
      senseiRequest.addSortField(convertSortField(sortField));
    }

    senseiRequest.setFacetSpecs(convertFacetSpecs(senseiProtoRequest.getFacetSpecList()));

    if (senseiProtoRequest.hasSenseiQuery()) {
      String senseiQueryString = senseiProtoRequest.getSenseiQuery();
      SenseiQuery senseiQuery = null;

      try {
        com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(senseiQueryString);
        senseiQuery = new SenseiJSONQuery(new JSONUtil.FastJSONObject(jsonObject));
      } catch (Exception e) {
        senseiQuery = new SenseiQuery(senseiQueryString.getBytes(SenseiQuery.UTF_8_CHARSET));
      }

      senseiRequest.setQuery(senseiQuery);
    }

    if (senseiProtoRequest.hasOffset())
      senseiRequest.setOffset(senseiProtoRequest.getOffset());

    if (senseiProtoRequest.hasCount())
      senseiRequest.setCount(senseiProtoRequest.getCount());

    if (senseiProtoRequest.hasFetchStoredFields())
      senseiRequest.setFetchStoredFields(senseiProtoRequest.getFetchStoredFields());

    if (senseiProtoRequest.hasFetchStoredValue())
      senseiRequest.setFetchStoredValue(senseiProtoRequest.getFetchStoredValue());

    if (senseiProtoRequest.hasFacetHandlerParam())
      senseiRequest.setFacetHandlerInitParamMap(convertFacetParams(senseiProtoRequest.getFacetHandlerParam()));

    if (senseiProtoRequest.getPartitionsCount() > 0)
      senseiRequest.setPartitions(new HashSet<Integer>(senseiProtoRequest.getPartitionsList()));

    if (senseiProtoRequest.hasExplain())
      senseiRequest.setShowExplanation(senseiProtoRequest.getExplain());

    if (senseiProtoRequest.hasRouteParam())
      senseiRequest.setRouteParam(senseiProtoRequest.getRouteParam());

    if (senseiProtoRequest.getGroupByCount() > 0) {
      List<String> groupByList = senseiProtoRequest.getGroupByList();
      senseiRequest.setGroupBy(groupByList.toArray(new String[]{}));
    }

    if (senseiProtoRequest.getDistinctCount() > 0)
      senseiRequest.setDistinct(senseiProtoRequest.getDistinctList().toArray(new String[]{}));

    if (senseiProtoRequest.hasMaxPerGroup())
      senseiRequest.setMaxPerGroup(senseiProtoRequest.getMaxPerGroup());

    if (senseiProtoRequest.getTermVectorsToFetchCount() > 0) {
      senseiRequest.setTermVectorsToFetch(new HashSet<String>(senseiProtoRequest.getTermVectorsToFetchList()));
    }

    if (senseiProtoRequest.getSelectListCount() > 0) {
      senseiRequest.setSelectList(senseiProtoRequest.getSelectListList());
    }

    if (senseiProtoRequest.hasMapReduce()) {

      SenseiMapReduce mapReduceFunction = convertMapReduce(senseiProtoRequest.getMapReduce());
      if (mapReduceFunction != null) {
        senseiRequest.setMapReduceFunction(mapReduceFunction);
        List<String> columns = senseiProtoRequest.getMapReduceColumnsList();
        JSONUtil.FastJSONObject jsonObject = new JSONUtil.FastJSONObject();
        try {
          jsonObject.put("column", columns.get(0));
          jsonObject.put("columns", columns.toArray(new String[]{}));
        } catch (JSONException e) {
          throw new IllegalStateException(e);
        }
        senseiRequest.getMapReduceFunction().init(jsonObject);
      } else {
        ByteString mapReduceBytes = senseiProtoRequest.getMapReduceBytes();

        Object object = null;
        try {
          ObjectInputStream ois = new ObjectInputStream(mapReduceBytes.newInput());
          object = ois.readObject();
        } catch (IOException ioException) {
          // Shouldn't happen.
          logger.error("IO Exception deserializing map reduce, ignoring mapreduce", ioException);
        } catch (ClassNotFoundException cnfe) {
          logger.error("Could not find class to deserialize to, ignoring mapreduce", cnfe);
        }

        if (object != null) {
          SenseiMapReduce mapReduce = (SenseiMapReduce) object;
          senseiRequest.setMapReduceFunction(mapReduce);
        }
      }
    }

    return senseiRequest;
  }

  @Override
  public byte[] requestToBytes(SenseiRequest request) {
    SenseiProtos.SenseiProtoRequest.Builder builder = SenseiProtos.SenseiProtoRequest.newBuilder();

    if (request.getSelections() != null) {
      for (BrowseSelection browseSelection : request.getSelections()) {
        builder.addSelection(convertBrowseSelection(browseSelection));
      }
    }

    for (SortField sortField : request.getSort()) {
      builder.addSortSpec(convertSortField(sortField));
    }

    if (request.getFacetSpecs() != null) {
      for (Map.Entry<String, FacetSpec> facetSpec : request.getFacetSpecs().entrySet()) {
        builder.addFacetSpec(convertFacetSpec(facetSpec.getKey(), facetSpec.getValue()));
      }
    }

    if (request.getQuery() != null) {
      SenseiQuery senseiQuery = request.getQuery();
      builder.setSenseiQuery(senseiQuery.toString());
    }

    builder
        .setOffset(request.getOffset())
        .setCount(request.getCount())
        .setFetchStoredFields(request.isFetchStoredFields())
        .setFetchStoredValue(request.isFetchStoredValue())
        .setFacetHandlerParam(convertFacetParams(request.getFacetHandlerInitParamMap()))
        .addAllPartitions(request.getPartitions() == null ? Collections.<Integer>emptyList() : request.getPartitions())
        .setExplain(request.isShowExplanation())
        .setRouteParam(request.getRouteParam())
        .setMaxPerGroup(request.getMaxPerGroup());

    if (request.getGroupBy() != null) {
      builder.addAllGroupBy(Arrays.asList(request.getGroupBy()));
    }

    if(request.getDistinct() != null) {
      builder.addAllDistinct(Arrays.asList(request.getDistinct()));
    }

    if(request.getTermVectorsToFetch() != null) {
      builder.addAllTermVectorsToFetch(request.getTermVectorsToFetch());
    }

    if(request.getSelectList() != null) {
      builder.addAllSelectList(request.getSelectList());
    }

    if (request.getMapReduceFunction() != null) {
      SenseiProtos.SenseiMapReduceFunction protoMapReduceFunction = convertMapReduce(request.getMapReduceFunction());
      builder.setMapReduce(protoMapReduceFunction);

      if (protoMapReduceFunction == SenseiProtos.SenseiMapReduceFunction.UNKNOWN) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
          ObjectOutputStream oos = new ObjectOutputStream(baos);
          oos.writeObject(request.getMapReduceFunction());
          oos.flush();
        } catch (IOException e) {
          logger.error("Could not deserialize map reduce function", e);
        }

        builder.setMapReduceBytes(ByteString.copyFrom(baos.toByteArray()));
      } else {
        String[] columns = request.getMapReduceFunction().getColumns();
        builder.addAllMapReduceColumns(Arrays.asList(columns));
      }
    }

    SenseiProtos.SenseiProtoRequest senseiProtoRequest = builder.build();
    return senseiProtoRequest.toByteArray();
  }

  private SenseiProtos.JavaPrimitives getPrimitiveType(Object[] array) {
    if (array instanceof Integer[]) {
      return SenseiProtos.JavaPrimitives.INT;
    } else if (array instanceof Long[]) {
      return SenseiProtos.JavaPrimitives.LONG;
    } else if (array instanceof Byte[]) {
      return SenseiProtos.JavaPrimitives.BYTE;
    } else if (array instanceof Character[]) {
      return SenseiProtos.JavaPrimitives.CHAR;
    } else if (array instanceof Float[]) {
      return SenseiProtos.JavaPrimitives.FLOAT;
    } else if (array instanceof Double[]) {
      return SenseiProtos.JavaPrimitives.DOUBLE;
    } else if (array instanceof Integer[]) {
      return SenseiProtos.JavaPrimitives.INT;
    } else if (array instanceof Short[]) {
      return SenseiProtos.JavaPrimitives.SHORT;
    } else if (array instanceof String[]) {
      return SenseiProtos.JavaPrimitives.STRING;
    } else {
      // Try to figure if the individual elements in the array are primitive and homogenous
      int[] counts = new int[SenseiProtos.JavaPrimitives.values().length + 1];

      for(Object o : array) {
        SenseiProtos.JavaPrimitives primitiveType = getPrimitiveType(o);
        counts[primitiveType.getNumber()]++;
      }

      int uniqueCount = 0;
      int primitiveIndex = 0;

      for(int i = 0; i < counts.length; i++) {
        int count = counts[i];
        if(count > 0) {
          primitiveIndex = i;
          uniqueCount++;
        }
      }

      if(uniqueCount == 1) {
        return SenseiProtos.JavaPrimitives.valueOf(primitiveIndex);
      } else {
        return SenseiProtos.JavaPrimitives.OBJECT;
      }
    }
  }

  private SenseiProtos.JavaPrimitives getPrimitiveType(Object o) {
    if (o instanceof Integer) {
      return SenseiProtos.JavaPrimitives.INT;
    } else if (o instanceof Long) {
      return SenseiProtos.JavaPrimitives.LONG;
    } else if (o instanceof Byte) {
      return SenseiProtos.JavaPrimitives.BYTE;
    } else if (o instanceof Character) {
      return SenseiProtos.JavaPrimitives.CHAR;
    } else if (o instanceof Float) {
      return SenseiProtos.JavaPrimitives.FLOAT;
    } else if (o instanceof Double) {
      return SenseiProtos.JavaPrimitives.DOUBLE;
    } else if (o instanceof Short) {
      return SenseiProtos.JavaPrimitives.SHORT;
    } else if (o instanceof String) {
      return SenseiProtos.JavaPrimitives.STRING;
    } else {
      return SenseiProtos.JavaPrimitives.OBJECT;
    }
  }

  private Object[] convert(List<String> values, SenseiProtos.JavaPrimitives type) {
    switch (type) {
      case BOOLEAN: {
        Boolean[] result = new Boolean[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Boolean.parseBoolean(values.get(i));
        return result;
      }
      case INT: {
        Integer[] result = new Integer[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Integer.parseInt(values.get(i));
        return result;
      }
      case LONG: {
        Long[] result = new Long[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Long.parseLong(values.get(i));
        return result;
      }
      case BYTE: {
        Byte[] result = new Byte[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Byte.parseByte(values.get(i));
        return result;
      }
      case CHAR: {
        Character[] result = new Character[values.size()];
        for (int i = 0; i < result.length; i++) {
          result[i] = values.get(i).charAt(0);
        }
        return result;
      }
      case FLOAT: {
        Float[] result = new Float[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Float.parseFloat(values.get(i));
        return result;
      }
      case DOUBLE: {
        Double[] result = new Double[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Double.parseDouble(values.get(i));
        return result;
      }
      case SHORT: {
        Short[] result = new Short[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = Short.parseShort(values.get(i));
        return result;
      }
      case STRING: {
        String[] result = new String[values.size()];
        for (int i = 0; i < result.length; i++) result[i] = values.get(i);
        return result;
      }
      default: {
        String error = "Unknown type for converting raw field values " + type;
        throw new IllegalArgumentException(error);
      }
    }
  }

  private Object convert(String value, SenseiProtos.JavaPrimitives type) {
    switch (type) {
      case BOOLEAN: return Boolean.parseBoolean(value);
      case INT: return Integer.parseInt(value);
      case LONG: return Long.parseLong(value);
      case BYTE: return Byte.parseByte(value);
      case CHAR: return value.charAt(0);
      case FLOAT: return Float.parseFloat(value);
      case DOUBLE: return Double.parseDouble(value);
      case SHORT: return Short.parseShort(value);
      case STRING: return value;
      default: {
        String error = "Unknown type for converting raw field values " + type;
        throw new IllegalArgumentException(error);
      }
    }

  }

  private SenseiProtos.Explanation convertExplanation(Explanation explanation) {
    SenseiProtos.Explanation.Builder builder = SenseiProtos.Explanation.newBuilder();

    builder.setDescription(explanation.getDescription());
    builder.setValue(explanation.getValue());
    if (explanation.getDetails() != null && explanation.getDetails().length > 0) {
      for (int i = 0; i < explanation.getDetails().length; i++) {
        builder.addDetails(convertExplanation(explanation.getDetails()[i]));
      }
    }

    return builder.build();
  }

  private Explanation convertExplanation(SenseiProtos.Explanation protoExplanation) {
    Explanation explanation = new Explanation(protoExplanation.getValue(), protoExplanation.getDescription());
    if (protoExplanation.getDetailsCount() > 0) {
      for (SenseiProtos.Explanation detail : protoExplanation.getDetailsList()) {
        explanation.addDetail(convertExplanation(detail));
      }
    }
    return explanation;
  }

  private SenseiProtos.Fieldable convertFieldable(Fieldable fieldable) {
    SenseiProtos.Fieldable.Builder builder = SenseiProtos.Fieldable.newBuilder();
    builder.setBoost(fieldable.getBoost());
    builder.setName(fieldable.name());

    if (fieldable.stringValue() != null)
      builder.setStringValue(fieldable.stringValue());

    builder.setStored(fieldable.isStored());
    builder.setIndexed(fieldable.isIndexed());
    builder.setTokenized(fieldable.isTokenized());
    builder.setTermVectorStored(fieldable.isTermVectorStored());
    builder.setStoreOffsetWithTermVector(fieldable.isStoreOffsetWithTermVector());
    builder.setStorePositionWithTermVector(fieldable.isStorePositionWithTermVector());
    builder.setBinary(fieldable.isBinary());
    builder.setOmitNorms(fieldable.getOmitNorms());
    builder.setLazy(fieldable.isLazy());
    builder.setBinaryOffset(fieldable.getBinaryOffset());
    builder.setBinaryLength(fieldable.getBinaryLength());
    if (fieldable.getBinaryValue() != null) {
      builder.setBinaryValue(ByteString.copyFrom(fieldable.getBinaryValue()));
    }
    return builder.build();
  }

  private Fieldable convertFieldable(SenseiProtos.Fieldable protoFieldable) {
    String name = protoFieldable.hasName() ? protoFieldable.getName() : null;
    String value = protoFieldable.hasStringValue() ? protoFieldable.getStringValue() : null;

    if (value != null) {
      Field.Store store = protoFieldable.hasStored() ? (protoFieldable.getStored() ? Field.Store.YES : Field.Store.NO) : Field.Store.NO;

      Field.TermVector termVector = Field.TermVector.toTermVector(protoFieldable.getStored(),
          protoFieldable.getStoreOffsetWithTermVector(),
          protoFieldable.getStorePositionWithTermVector());

      Field.Index index = Field.Index.toIndex(protoFieldable.getIndexed(), false, protoFieldable.getOmitNorms());

      Field field = new Field(name, value, store, index, termVector);

      if (protoFieldable.hasBoost())
        field.setBoost(protoFieldable.getBoost());

      if (protoFieldable.hasOmitNorms())
        field.setOmitNorms(protoFieldable.getOmitNorms());

      return field;
    } else {
      Field.Store store = protoFieldable.hasStored() ? (protoFieldable.getStored() ? Field.Store.YES : Field.Store.NO) : Field.Store.NO;
      byte[] binaryValue = protoFieldable.getBinaryValue() == null ? null : protoFieldable.getBinaryValue().toByteArray();
      int binaryOffset = protoFieldable.getBinaryOffset();
      int binaryLength = protoFieldable.getBinaryLength();

      Field field = new Field(name, binaryValue, binaryOffset, binaryLength, store);
      return field;
    }
  }

  private SenseiProtos.Document convertDocument(Document document) {
    SenseiProtos.Document.Builder builder = SenseiProtos.Document.newBuilder();
    builder.setBoost(document.getBoost());

    if (document.getFields() != null) {
      for (Fieldable field : document.getFields()) {
        builder.addFields(convertFieldable(field));
      }
    }
    return builder.build();
  }

  private Document convertDocument(SenseiProtos.Document protoDocument) {
    Document document = new Document();
    if (protoDocument.hasBoost())
      document.setBoost(protoDocument.getBoost());

    if (protoDocument.getFieldsCount() > 0) {
      for (SenseiProtos.Fieldable protoFieldable : protoDocument.getFieldsList()) {
        document.add(convertFieldable(protoFieldable));
      }
    }
    return document;
  }

  private SenseiProtos.TermFrequencyMap convert(Map<String, BrowseHit.TermFrequencyVector> map) {
    SenseiProtos.TermFrequencyMap.Builder builder = SenseiProtos.TermFrequencyMap.newBuilder();
    for(Map.Entry<String, BrowseHit.TermFrequencyVector> entry : map.entrySet()) {
      SenseiProtos.TermFrequencyVector.Builder vectorBuilder = SenseiProtos.TermFrequencyVector.newBuilder();

      BrowseHit.TermFrequencyVector vector = entry.getValue();
      for(int i = 0; i < vector.freqs.length; i++) {
        vectorBuilder.addFreq(vector.freqs[i]);
        vectorBuilder.addTerms(vector.terms[i]);
      }

      builder.addKey(entry.getKey());
      builder.addValue(vectorBuilder);
    }
    return builder.build();
  }

  private Map<String, BrowseHit.TermFrequencyVector> convert(SenseiProtos.TermFrequencyMap protoMap) {
    HashMap<String, BrowseHit.TermFrequencyVector> result = new HashMap<String, BrowseHit.TermFrequencyVector>();
    for(int i = 0; i < protoMap.getKeyCount(); i++) {
      SenseiProtos.TermFrequencyVector protoVector = protoMap.getValue(i);
      int[] freq = new int[protoVector.getFreqCount()];
      String[] terms = new String[protoVector.getTermsCount()];

      for(int j = 0; j < protoVector.getFreqCount(); j++) {
        freq[j] = protoVector.getFreq(j);
      }

      for(int j = 0; j < protoVector.getTermsCount(); j++) {
        terms[j] = protoVector.getTerms(j);
      }

      BrowseHit.TermFrequencyVector termFrequencyVector = new BrowseHit.TermFrequencyVector(terms, freq);
      result.put(protoMap.getKey(i), termFrequencyVector);
    }
    return result;
  }

  private SenseiProtos.ObjectArray convert(Object[] objects) {
    SenseiProtos.ObjectArray.Builder builder = SenseiProtos.ObjectArray.newBuilder();
    SenseiProtos.JavaPrimitives primitiveType = getPrimitiveType(objects);
    builder.setType(primitiveType);

    switch (primitiveType) {
      case BYTE: {
        for(Object o : objects) {
          builder.addIntValue((Byte) o);
        }
        break;
      }
      case SHORT: {
        for(Object o : objects) {
          builder.addIntValue((Short) o);
        }
        break;
      }
      case CHAR: {
        for(Object o : objects) {
          builder.addIntValue((Character) o);
        }
        break;
      }
      case INT: {
        for(Object o : objects) {
          builder.addIntValue((Integer) o);
        }
        break;
      }
      case BOOLEAN: {
        for(Object o : objects) {
          builder.addBooleanValue((Boolean) o);
        }
        break;
      }
      case LONG: {
        for(Object o : objects) {
          builder.addLongValue((Long) o);
        }
        break;
      }
      case FLOAT: {
        for(Object o : objects) {
          builder.addFloatValue((Float) o);
        }
        break;
      }
      case DOUBLE: {
        for(Object o : objects) {
          builder.addDoubleValue((Double) o);
        }
        break;
      }
      case STRING: {
        for(Object o : objects) {
          builder.addStringValue((String) o);
        }
        break;
      }
      case OBJECT: {
        for(Object o : objects) {
          if(o instanceof long[]) {
            SenseiProtos.LongArray.Builder longArrayBuilder = SenseiProtos.LongArray.newBuilder();
            for(long item : (long[]) o) {
              longArrayBuilder.addItem(item);
            }
            builder.addLongArray(longArrayBuilder);
          } else {
            builder.addObjectValue(javaSerialize(o));
          }
        }
        break;
      }
    }
    return builder.build();
  }

  private Object[] convert(SenseiProtos.ObjectArray objectArray) {
    SenseiProtos.JavaPrimitives type = objectArray.getType();
    switch (type) {
      case BOOLEAN: {
        Boolean[] result = new Boolean[objectArray.getBooleanValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getBooleanValue(i);
        }
        return result;
      }
      case BYTE: {
        Byte[] result = new Byte[objectArray.getIntValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = (byte) objectArray.getIntValue(i);
        }
        return result;
      }
      case CHAR: {
        Character[] result = new Character[objectArray.getIntValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = (char) objectArray.getIntValue(i);
        }
        return result;
      }
      case SHORT: {
        Short[] result = new Short[objectArray.getIntValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = (short) objectArray.getIntValue(i);
        }
        return result;
      }
      case INT: {
        Integer[] result = new Integer[objectArray.getIntValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getIntValue(i);
        }
        return result;
      }
      case LONG: {
        Long[] result = new Long[objectArray.getLongValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getLongValue(i);
        }
        return result;
      }
      case FLOAT: {
        Float[] result = new Float[objectArray.getFloatValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getFloatValue(i);
        }
        return result;
      }
      case DOUBLE: {
        Double[] result = new Double[objectArray.getDoubleValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getDoubleValue(i);
        }
        return result;
      }
      case STRING: {
        String[] result = new String[objectArray.getStringValueCount()];
        for(int i = 0; i < result.length; i++) {
          result[i] = objectArray.getStringValue(i);
        }
        return result;
      }
      case OBJECT: {
        if(objectArray.getLongArrayCount() > 0) {
          long[][] result = new long[objectArray.getLongArrayCount()][];
          for(int i = 0; i < result.length; i++) {
            result[i] = new long[objectArray.getLongArray(i).getItemCount()];
            for(int j = 0; j < result[i].length; j++) {
              result[i][j] = objectArray.getLongArray(i).getItem(j);
            }
          }
          return result;
        } else {
          Object[] result = new Object[objectArray.getObjectValueCount()];
          for(int i = 0; i < result.length; i++) {
            result[i] = javaDeserialize(objectArray.getObjectValue(i));
          }
          return result;
        }
      }
      default: {
          Object[] result = new Object[objectArray.getObjectValueCount()];
          for(int i = 0; i < result.length; i++) {
              result[i] = javaDeserialize(objectArray.getObjectValue(i));
          }
          return result;
      }
    }
  }

  private SenseiProtos.SenseiHit convert(SenseiHit senseiHit) {
    SenseiProtos.SenseiHit.Builder builder = SenseiProtos.SenseiHit.newBuilder();
    builder.setScore(senseiHit.getScore());
    builder.setDocId(senseiHit.getDocid());

    if(senseiHit.getRawFieldValues() != null) {
      SenseiProtos.FieldValues.Builder fieldValues = SenseiProtos.FieldValues.newBuilder();
      for(Map.Entry<String, Object[]> entry : senseiHit.getRawFieldValues().entrySet()) {
        fieldValues.addKey(entry.getKey());
        fieldValues.addRawValue(convert(entry.getValue()));

        String[] fields = senseiHit.getFields(entry.getKey());
        fieldValues.addValue(convert(fields));
      }
      builder.setFieldValues(fieldValues);
    }

    builder.setGroupPosition(senseiHit.getGroupPosition());

    if (senseiHit.getGroupField() != null)
      builder.setGroupField(senseiHit.getGroupField());

    if (senseiHit.getGroupValue() != null) {
      builder.setGroupValue(senseiHit.getGroupValue());
    }

    if(senseiHit.getRawGroupValue() != null) {
      builder.setRawGroupValue(javaSerialize(senseiHit.getRawGroupValue()));
    }

    builder.setTotalGroupHitCount(senseiHit.getGroupHitsCount());

    if (senseiHit.getGroupHits() != null && senseiHit.getSenseiGroupHits().length > 0) {
      for (BrowseHit groupHit : senseiHit.getSenseiGroupHits()) {
        if(groupHit instanceof SenseiHit) {
          SenseiProtos.SenseiHit protoGroupHit = convert((SenseiHit) groupHit);
          builder.addGroupHit(protoGroupHit);
        } else {
          throw new IllegalArgumentException("Expect the browse hits to be of type SenseiHit");
        }
      }
    }

    if (senseiHit.getExplanation() != null) {
      builder.setExplanation(convertExplanation(senseiHit.getExplanation()));
    }

    builder.setUid(senseiHit.getUID());
    if (senseiHit.getSrcData() != null) {
      builder.setSrcData(senseiHit.getSrcData());
    }

    if (senseiHit.getStoredFields() != null) {
      builder.setStoredFields(convertDocument(senseiHit.getStoredFields()));
    }

    if (senseiHit.getStoredValue() != null)
      builder.setStoredValue(ByteString.copyFrom(senseiHit.getStoredValue()));

    if(senseiHit.getTermFreqMap() != null) {
      builder.setTermFrequencyMap(convert(senseiHit.getTermFreqMap()));
    }

//    builder.setSenseiHitBytes(javaSerialize(senseiHit));

    return builder.build();
  }

  private String[] convert(SenseiProtos.StringArray protoStringArray) {
    String[] result = new String[protoStringArray.getItemCount()];
    for(int i = 0; i < result.length; i++) {
      result[i] = protoStringArray.getItem(i);
    }
    return result;
  }

  private SenseiProtos.StringArray convert(String[] stringArray) {
    SenseiProtos.StringArray.Builder builder = SenseiProtos.StringArray.newBuilder();
    for(String string : stringArray) {
      builder.addItem(string);
    }
    return builder.build();
  }

  private SenseiHit convert(SenseiProtos.SenseiHit protoSenseiHit) {
    SenseiHit senseiHit = new SenseiHit();
    if (protoSenseiHit.hasScore()) {
      senseiHit.setScore(protoSenseiHit.getScore());
    }

    if (protoSenseiHit.hasDocId()) {
      senseiHit.setDocid(protoSenseiHit.getDocId());
    }

    SenseiProtos.FieldValues protoFieldValues = protoSenseiHit.getFieldValues();
    Map<String, Object[]> rawFieldValues = new LinkedHashMap<String, Object[]>();
    Map<String, String[]> fieldValues = new LinkedHashMap<String, String[]>();
    if(protoFieldValues != null) {
      int numItems = protoFieldValues.getKeyCount();

      if(numItems > 0) {
        for(int i = 0; i < numItems; i++) {
          String key = protoFieldValues.getKey(i);
          SenseiProtos.ObjectArray protoObjectArray = protoFieldValues.getRawValue(i);
          Object[] objects = convert(protoObjectArray);

          rawFieldValues.put(key, objects);

          fieldValues.put(key, convert(protoFieldValues.getValue(i)));
        }

      }
    }
    senseiHit.setRawFieldValues(rawFieldValues);
    senseiHit.setFieldValues(fieldValues);


    if (protoSenseiHit.hasGroupPosition()) {
      senseiHit.setGroupPosition(protoSenseiHit.getGroupPosition());
    }

    if (protoSenseiHit.hasGroupField()) {
      senseiHit.setGroupField(protoSenseiHit.getGroupField());
    }

    if (protoSenseiHit.hasGroupValue()) {
      senseiHit.setGroupValue(protoSenseiHit.getGroupValue());
    }

    if(protoSenseiHit.hasRawGroupValue()) {
      senseiHit.setRawGroupValue(javaDeserialize(protoSenseiHit.getRawGroupValue()));
    }

    if(protoSenseiHit.hasTotalGroupHitCount()) {
      senseiHit.setGroupHitsCount(protoSenseiHit.getTotalGroupHitCount());
    }

    if (protoSenseiHit.getGroupHitCount() > 0) {
      SenseiHit[] groupHits = new SenseiHit[protoSenseiHit.getGroupHitCount()];
      int i = 0;
      for (SenseiProtos.SenseiHit hit : protoSenseiHit.getGroupHitList()) {
        groupHits[i++] = convert(hit);
      }
      senseiHit.setGroupHits(groupHits);
    }

    if (protoSenseiHit.hasExplanation()) {
      senseiHit.setExplanation(convertExplanation(protoSenseiHit.getExplanation()));
    }

    if (protoSenseiHit.hasUid()) {
      senseiHit.setUID(protoSenseiHit.getUid());
    }

    if (protoSenseiHit.hasSrcData()) {
      senseiHit.setSrcData(protoSenseiHit.getSrcData());
    }

    if (protoSenseiHit.hasStoredFields()) {
      senseiHit.setStoredFields(convertDocument(protoSenseiHit.getStoredFields()));
    }

    if (protoSenseiHit.hasStoredValue()) {
      senseiHit.setStoredValue(protoSenseiHit.getStoredValue().toByteArray());
    }

    if (protoSenseiHit.hasTermFrequencyMap()) {
      senseiHit.setTermFreqMap(convert(protoSenseiHit.getTermFrequencyMap()));
    }

    return senseiHit;
  }

  private BrowseFacet convertBrowseFacet(SenseiProtos.BrowseFacet protoFacet) {
    BrowseFacet facet = new BrowseFacet();
    facet.setFacetValueHitCount(protoFacet.getHitCount());

    if (protoFacet.hasValue())
      facet.setValue(protoFacet.getValue());
    return facet;
  }

  private SenseiProtos.BrowseFacet convertBrowseFacet(BrowseFacet browseFacet) {
    SenseiProtos.BrowseFacet.Builder builder = SenseiProtos.BrowseFacet.newBuilder();
    builder.setHitCount(browseFacet.getFacetValueHitCount());
    if (browseFacet.getValue() != null)
      builder.setValue(browseFacet.getValue());

    return builder.build();
  }

  private SenseiProtos.FacetAccessible convertFacetAccessible(FacetAccessible facetAccessible) {
    SenseiProtos.FacetAccessible.Builder builder = SenseiProtos.FacetAccessible.newBuilder();

//    ByteString byteString = javaSerialize(facetAccessible);
//    builder.setFacetAccessibleBytes(byteString);

    List<BrowseFacet> facets = facetAccessible.getFacets();
    for (BrowseFacet facet : facets) {
      SenseiProtos.BrowseFacet protoFacet = convertBrowseFacet(facet);
      builder.addFacets(protoFacet);
    }
    return builder.build();
  }

  private FacetAccessible convertFacetAccessible(SenseiProtos.FacetAccessible facetAccessible) {
//    ByteString facetAccessibleBytes = facetAccessible.getFacetAccessibleBytes();
//    return (FacetAccessible) javaDeserialize(facetAccessibleBytes);
    List<SenseiProtos.BrowseFacet> protoFacets = facetAccessible.getFacetsList();
    BrowseFacet[] facets = new BrowseFacet[protoFacets.size()];

    int i = 0;
    for (SenseiProtos.BrowseFacet protoFacet : protoFacets) {
      BrowseFacet browseFacet = convertBrowseFacet(protoFacet);
      facets[i++] = browseFacet;
    }

    return new MappedFacetAccessible(facets);
  }

  private SenseiProtos.FacetMap convertFacetMap(Map<String, FacetAccessible> facetMap) {
    SenseiProtos.FacetMap.Builder builder = SenseiProtos.FacetMap.newBuilder();
    for (Map.Entry<String, FacetAccessible> entry : facetMap.entrySet()) {
      builder.addKey(entry.getKey());
      builder.addValue(convertFacetAccessible(entry.getValue()));
    }
    return builder.build();
  }

  private Map<String, FacetAccessible> convertFacetMap(SenseiProtos.FacetMap protoFacetMap) {
    Map<String, FacetAccessible> facetMap = new HashMap<String, FacetAccessible>();
    for (int i = 0; i < protoFacetMap.getKeyCount(); i++) {
      facetMap.put(protoFacetMap.getKey(i), convertFacetAccessible(protoFacetMap.getValue(i)));

    }
    return facetMap;
  }

  private static final ByteString javaSerialize(Object o) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos);
      objectOutputStream.writeObject(o);
      objectOutputStream.close();
    } catch (IOException ioException) {
      logger.error("Could not serialize java object ", ioException);
    }
    return ByteString.copyFrom(baos.toByteArray());
  }

  private static final Object javaDeserialize(ByteString bytes) {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes.toByteArray());

    Object object = null;
    try {
      ObjectInputStream objectInputStream = new ObjectInputStream(bais);
      object = objectInputStream.readObject();
    } catch (IOException ioException) {
      logger.error("Could not deserialize reduce result ", ioException);
    } catch (ClassNotFoundException cnfe) {
      logger.error("Unknown class trying to deserialize reduce result ", cnfe);
    }
    return object;
  }


  private SenseiProtos.MapReduceResult convertMapReduceResult(MapReduceResult mapReduceResult) {
    SenseiProtos.MapReduceResult.Builder builder = SenseiProtos.MapReduceResult.newBuilder();

    List mapResults = mapReduceResult.getMapResults();
    Serializable reduceResult = mapReduceResult.getReduceResult();

    if (reduceResult != null) {
      builder.setReduceResult(javaSerialize(reduceResult));
    }

    if (mapResults != null) {
      for (Object mapResult : mapResults) {
        builder.addMapResult(javaSerialize(mapResult));
      }
    }

    return builder.build();
  }

  private MapReduceResult convertMapReduceResult(SenseiProtos.MapReduceResult protoMapReduceResult) {
    MapReduceResult mapReduceResult = new MapReduceResult();

    if (protoMapReduceResult.hasReduceResult()) {
      Object reduceResult = javaDeserialize(protoMapReduceResult.getReduceResult());

      if (reduceResult != null)
        mapReduceResult.setReduceResult((Serializable) reduceResult);
    }

    if (protoMapReduceResult.getMapResultCount() > 0) {
      List mapResults = new ArrayList();
      for (ByteString protoMapResult : protoMapReduceResult.getMapResultList()) {
        Object mapResult = javaDeserialize(protoMapResult);

        if (mapResult != null)
          mapResults.add(mapResult);
      }
      mapReduceResult.setMapResults(mapResults);
    }
    return mapReduceResult;
  }

  private SenseiProtos.ErrorType convert(ErrorType errorType) {
    switch (errorType) {
      case BoboExecutionError: return SenseiProtos.ErrorType.BoboExecutionError;
      case BQLParsingError: return SenseiProtos.ErrorType.BQLParsingError;
      case BrokerGatherError: return SenseiProtos.ErrorType.BrokerGatherError;
      case BrokerTimeout: return SenseiProtos.ErrorType.BrokerTimeout;
      case ExecutionTimeout: return SenseiProtos.ErrorType.ExecutionTimeout;
      case FederatedBrokerUnavailable: return SenseiProtos.ErrorType.FederatedBrokerUnavailable;
      case InternalError: return SenseiProtos.ErrorType.InternalError;
      case JsonCompilationError: return SenseiProtos.ErrorType.JsonCompilationError;
      case JsonParsingError: return SenseiProtos.ErrorType.JsonParsingError;
      case MergePartitionError: return SenseiProtos.ErrorType.MergePartitionError;
      case PartitionCallError: return SenseiProtos.ErrorType.PartitionCallError;
      case UnknownError: return SenseiProtos.ErrorType.UnknownError;
      default: {
        logger.warn("Unknown error type in proto serialization, setting unknown " + errorType);
        return SenseiProtos.ErrorType.UnknownError;
      }
    }
  }

  private ErrorType convert(SenseiProtos.ErrorType protoErrorType) {
    switch (protoErrorType) {
      case BoboExecutionError: return ErrorType.BoboExecutionError;
      case BQLParsingError: return ErrorType.BQLParsingError;
      case BrokerGatherError: return ErrorType.BrokerGatherError;
      case BrokerTimeout: return ErrorType.BrokerTimeout;
      case ExecutionTimeout: return ErrorType.ExecutionTimeout;
      case FederatedBrokerUnavailable: return ErrorType.FederatedBrokerUnavailable;
      case InternalError: return ErrorType.InternalError;
      case JsonCompilationError: return ErrorType.JsonCompilationError;
      case JsonParsingError: return ErrorType.JsonParsingError;
      case MergePartitionError: return ErrorType.MergePartitionError;
      case PartitionCallError: return ErrorType.PartitionCallError;
      case UnknownError: return ErrorType.UnknownError;
      default: {
        logger.warn("Unknown serialization error coming from protobuf, setting unknown " + protoErrorType);
        return ErrorType.UnknownError;
      }
    }
  }

  private SenseiProtos.SenseiError convert(SenseiError senseiError) {
    SenseiProtos.SenseiError.Builder builder = SenseiProtos.SenseiError.newBuilder();

    builder.setCode(senseiError.getErrorCode());

    if(senseiError.getMessage() != null) {
      builder.setMessage(senseiError.getMessage());
    }

    if(senseiError.getErrorType() != null) {
      builder.setType(convert(senseiError.getErrorType()));
    }

    return builder.build();
  }

  private SenseiError convert(SenseiProtos.SenseiError protoSenseiError) {
    String message = protoSenseiError.hasMessage() ? protoSenseiError.getMessage() : null;
    ErrorType errorType = protoSenseiError.hasType() ? convert(protoSenseiError.getType()) : null;
    int code = protoSenseiError.hasCode() ? protoSenseiError.getCode() :
        (errorType != null ? errorType.getDefaultErrorCode() : 0);

    SenseiError senseiError =
        new SenseiError(message, errorType, code);

    return senseiError;
  }


  @Override
  public byte[] responseToBytes(SenseiResult senseiResult) {
    SenseiProtos.SenseiProtoResult.Builder builder = SenseiProtos.SenseiProtoResult.newBuilder();

    if (senseiResult.getParsedQuery() != null)
      builder.setParsedQuery(senseiResult.getParsedQuery());

    if (senseiResult.getSenseiHits() != null && senseiResult.getSenseiHits().length > 0) {
      for (SenseiHit senseiHit : senseiResult.getSenseiHits()) {
        builder.addHit(convert(senseiHit));
      }
    }

    builder.setTid(senseiResult.getTid());
    builder.setNumHits(senseiResult.getNumHits());
    builder.setTotalDocs(senseiResult.getTotalDocs());

    FacetAccessible[] groupAccessibles = senseiResult.getGroupAccessibles();
    if (groupAccessibles != null && groupAccessibles.length > 0) {
      for (FacetAccessible groupAccessible : groupAccessibles) {
        builder.addGroupAccessible(convertFacetAccessible(groupAccessible));
      }
    }

    Map<String, FacetAccessible> facetMap = senseiResult.getFacetMap();
    if (facetMap != null) {
      builder.setFacetMap(convertFacetMap(facetMap));
    }

    builder.setTime(senseiResult.getTime());

    if (senseiResult.getMapReduceResult() != null)
      builder.setMapReduceResult(convertMapReduceResult(senseiResult.getMapReduceResult()));

    if(senseiResult.getErrors() != null && !senseiResult.getErrors().isEmpty()) {
      for(SenseiError senseiError : senseiResult.getErrors()) {
        builder.addError(convert(senseiError));
      }
    }

    SenseiProtos.SenseiProtoResult protoResult = builder.build();
    return protoResult.toByteArray();
  }

  @Override
  public SenseiResult responseFromBytes(byte[] bytes) {
    SenseiProtos.SenseiProtoResult senseiProtoResult = null;
    try {
      senseiProtoResult = SenseiProtos.SenseiProtoResult.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException("Could not parse proto result");
    }

    SenseiResult senseiResult = new SenseiResult();
    if (senseiProtoResult.hasParsedQuery())
      senseiResult.setParsedQuery(senseiProtoResult.getParsedQuery());

    List<SenseiHit> hits = new ArrayList<SenseiHit>(senseiProtoResult.getHitCount());
    for (SenseiProtos.SenseiHit senseiHit : senseiProtoResult.getHitList()) {
      hits.add(convert(senseiHit));
    }
    senseiResult.setHits(hits.toArray(new SenseiHit[]{}));

    if (senseiProtoResult.hasTid())
      senseiResult.setTid(senseiProtoResult.getTid());

    if (senseiProtoResult.hasNumHits())
      senseiResult.setNumHits((int) senseiProtoResult.getNumHits());

    if (senseiProtoResult.hasTotalDocs())
      senseiResult.setTotalDocs((int) senseiProtoResult.getTotalDocs());

    if (senseiProtoResult.getGroupAccessibleCount() > 0) {
      List<SenseiProtos.FacetAccessible> groupAccessibleList = senseiProtoResult.getGroupAccessibleList();
      FacetAccessible[] facetAccessibles = new FacetAccessible[groupAccessibleList.size()];
      int i = 0;
      for (SenseiProtos.FacetAccessible protoFacetAccessible : groupAccessibleList) {
        FacetAccessible facetAccessible = convertFacetAccessible(protoFacetAccessible);
        facetAccessibles[i++] = facetAccessible;
      }

      senseiResult.setGroupAccessibles(facetAccessibles);

    }

    if (senseiProtoResult.hasFacetMap()) {
      senseiResult.addAll(convertFacetMap(senseiProtoResult.getFacetMap()));
    }

    if (senseiProtoResult.hasTime())
      senseiResult.setTime(senseiProtoResult.getTime());

    if (senseiProtoResult.hasMapReduceResult())
      senseiResult.setMapReduceResult(convertMapReduceResult(senseiProtoResult.getMapReduceResult()));

    if (senseiProtoResult.getErrorCount() > 0) {
      List<SenseiProtos.SenseiError> errorList = senseiProtoResult.getErrorList();
      for(SenseiProtos.SenseiError error : errorList) {
        senseiResult.addError(convert(error));
      }
    }

    return senseiResult;
  }


  private SenseiProtos.BooleanOperator convertOperator(BrowseSelection.ValueOperation valueOperation) {
    switch (valueOperation) {
      case ValueOperationAnd:
        return SenseiProtos.BooleanOperator.AND;
      case ValueOperationOr:
        return SenseiProtos.BooleanOperator.OR;
      default:
        throw new IllegalArgumentException();
    }
  }

  private BrowseSelection.ValueOperation convertOperator(SenseiProtos.BooleanOperator valueOperation) {
    switch (valueOperation) {
      case AND:
        return BrowseSelection.ValueOperation.ValueOperationAnd;
      case OR:
        return BrowseSelection.ValueOperation.ValueOperationOr;
      default:
        throw new IllegalArgumentException();
    }
  }

  private SenseiProtos.StringProperties convertProperties(Map<?, ?> map) {
    SenseiProtos.StringProperties.Builder stringPropertiesBuilder = SenseiProtos.StringProperties.newBuilder();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      stringPropertiesBuilder.addKey(key);
      stringPropertiesBuilder.addValue(value);
    }
    return stringPropertiesBuilder.build();
  }

  private Map<String, String> convertProperties(SenseiProtos.StringProperties stringProperties) {
    Map<String, String> results = new HashMap<String, String>();
    for (int i = 0; i < stringProperties.getKeyCount(); i++) {
      results.put(stringProperties.getKey(i), stringProperties.getValue(i));
    }
    return results;
  }

  private SenseiProtos.BrowseSelection convertBrowseSelection(BrowseSelection browseSelection) {
    BrowseSelection.ValueOperation selectionOperation = browseSelection.getSelectionOperation();

    return SenseiProtos.BrowseSelection.newBuilder().setOperator(convertOperator(selectionOperation)).setFieldName(
        browseSelection.getFieldName()).addAllValue(browseSelection.getValues() == null ? Collections.<String>emptyList() : Arrays.asList(
        browseSelection.getValues())).addAllNotValue(browseSelection.getNotValues() == null ? Collections.<String>emptyList() : Arrays.asList(
        browseSelection.getNotValues())).setProperties(convertProperties(browseSelection.getSelectionProperties())).build();
  }

  private BrowseSelection convertBrowseSelection(SenseiProtos.BrowseSelection protoBrowseSelection) {
    BrowseSelection browseSelection = new BrowseSelection(protoBrowseSelection.getFieldName());
    browseSelection.setSelectionOperation(convertOperator(protoBrowseSelection.getOperator()));
    browseSelection.setValues(protoBrowseSelection.getValueList().toArray(new String[]{}));
    browseSelection.setNotValues(protoBrowseSelection.getNotValueList().toArray(new String[]{}));
    browseSelection.setSelectionProperties(convertProperties(protoBrowseSelection.getProperties()));

    return browseSelection;
  }

  private SenseiProtos.Locale convertLocale(Locale locale) {
    String country = locale.getCountry();
    String language = locale.getLanguage();
    String variant = locale.getVariant();

    SenseiProtos.Locale.Builder builder = SenseiProtos.Locale.newBuilder();
    if (country != null)
      builder.setCountry(country);

    if (language != null)
      builder.setLanguage(language);

    if (variant != null)
      builder.setVariant(variant);

    return builder.build();
  }

  private Locale convertLocale(SenseiProtos.Locale locale) {
    String country = locale.hasCountry() ? locale.getCountry() : "";
    String language = locale.hasLanguage() ? locale.getLanguage() : "";
    String variant = locale.hasVariant() ? locale.getVariant() : "";

    return new Locale(language, country, variant);
  }

  private SenseiProtos.SortField convertSortField(SortField sortField) {
    SenseiProtos.SortField.Builder builder = SenseiProtos.SortField.newBuilder();
    if (sortField.getField() != null)
      builder.setField(sortField.getField());

    builder.setType(sortField.getType());

    if (sortField.getLocale() != null)
      builder.setLocale(convertLocale(sortField.getLocale()));

    builder.setReverse(sortField.getReverse());

    return builder.build();
  }

  private SortField convertSortField(SenseiProtos.SortField protoSortField) {
    String field = protoSortField.hasField() ? protoSortField.getField() : null;
    SortField sortField = new SortField(field,
        protoSortField.getType(),
        protoSortField.getReverse());
    return sortField;
  }

  private SenseiProtos.SortOrder convertSortOrder(FacetSpec.FacetSortSpec facetSortSpec) {
    switch (facetSortSpec) {
      case OrderValueAsc:
        return SenseiProtos.SortOrder.ASCENDING;
      case OrderHitsDesc:
        return SenseiProtos.SortOrder.DESCENDING;
      case OrderByCustom:
        return SenseiProtos.SortOrder.CUSTOM;
      default:
        throw new IllegalArgumentException();
    }
  }

  private FacetSpec.FacetSortSpec convertSortOrder(SenseiProtos.SortOrder sortOrder) {
    switch (sortOrder) {
      case ASCENDING:
        return FacetSpec.FacetSortSpec.OrderValueAsc;
      case DESCENDING:
        return FacetSpec.FacetSortSpec.OrderHitsDesc;
      case CUSTOM:
        return FacetSpec.FacetSortSpec.OrderByCustom;
      default:
        throw new IllegalArgumentException();
    }
  }

  private SenseiProtos.FacetSpec convertFacetSpec(String field, FacetSpec facetSpec) {
    return SenseiProtos.FacetSpec.newBuilder().setOrderBy(convertSortOrder(facetSpec.getOrderBy())).setMax(facetSpec.getMaxCount()).setExpandSelection(
        facetSpec.isExpandSelection()).setMinCount(facetSpec.getMinHitCount()).setProperties(convertProperties(facetSpec.getProperties())).setField(
        field).build();
  }

  private Map<String, FacetSpec> convertFacetSpecs(Iterable<SenseiProtos.FacetSpec> facetSpecs) {
    Map<String, FacetSpec> result = new HashMap<String, FacetSpec>();
    for (SenseiProtos.FacetSpec protoFacetSpec : facetSpecs) {
      FacetSpec facetSpec = new FacetSpec();
      facetSpec.setOrderBy(convertSortOrder(protoFacetSpec.getOrderBy()));
      facetSpec.setMaxCount(protoFacetSpec.getMax());
      facetSpec.setExpandSelection(protoFacetSpec.getExpandSelection());
      facetSpec.setMinHitCount(protoFacetSpec.getMinCount());
      facetSpec.setProperties(convertProperties(protoFacetSpec.getProperties()));
      result.put(protoFacetSpec.getField(), facetSpec);
    }
    return result;
  }

  private SenseiProtos.SenseiMapReduceFunction convertMapReduce(SenseiMapReduce mapReduce) {
    if (mapReduce instanceof AvgMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.AVG;
    } else if (mapReduce instanceof CountGroupByMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.COUNT_GROUP_BY;
    } else if (mapReduce instanceof DistinctCountMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.DISTINCT_COUNT;
    } else if (mapReduce instanceof DistinctUIDCount) {
      return SenseiProtos.SenseiMapReduceFunction.DISTINCT_UID;
    } else if (mapReduce instanceof FacetCountsMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.FACET_COUNTS;
    } else if (mapReduce instanceof HashSetDistinctCountMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.HASH_SET_DISTINCT_COUNT;
    } else if (mapReduce instanceof MaxMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.MAX;
    } else if (mapReduce instanceof MinMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.MIN;
    } else if (mapReduce instanceof SumMapReduce) {
      return SenseiProtos.SenseiMapReduceFunction.SUM;
    } else {
      return SenseiProtos.SenseiMapReduceFunction.UNKNOWN;
    }
  }

  private SenseiMapReduce convertMapReduce(SenseiProtos.SenseiMapReduceFunction mapReduce) {
    switch (mapReduce) {
      case AVG:
        return new AvgMapReduce();
      case COUNT_GROUP_BY:
        return new CountGroupByMapReduce();
      case DISTINCT_COUNT:
        return new DistinctCountMapReduce();
      case DISTINCT_UID:
        return new DistinctUIDCount();
      case FACET_COUNTS:
        return new FacetCountsMapReduce();
      case HASH_SET_DISTINCT_COUNT:
        return new HashSetDistinctCountMapReduce();
      case MAX:
        return new MaxMapReduce();
      case MIN:
        return new MinMapReduce();
      case SUM:
        return new SumMapReduce();
      case UNKNOWN:
        return null;
      default:
        throw new IllegalArgumentException("Cannot translate " + mapReduce + " from protobuf");
    }
  }

  private SenseiProtos.FacetHandlerInitializerParams convertFacetParams(Map<String, FacetHandlerInitializerParam> facetHandlerInitializerParamMap) {
    SenseiProtos.FacetHandlerInitializerParams.Builder builder = SenseiProtos.FacetHandlerInitializerParams.newBuilder();
    for (Map.Entry<String, FacetHandlerInitializerParam> entry : facetHandlerInitializerParamMap.entrySet()) {
      builder.addKey(entry.getKey());
      builder.addValue(convertFacetParam(entry.getValue()));
    }
    return builder.build();
  }

  private Map<String, FacetHandlerInitializerParam> convertFacetParams(SenseiProtos.FacetHandlerInitializerParams protoFacetHandlerInitializerParams) {
    Map<String, FacetHandlerInitializerParam> result = new HashMap<String, FacetHandlerInitializerParam>();
    for (int i = 0; i < protoFacetHandlerInitializerParams.getKeyCount(); i++) {
      result.put(protoFacetHandlerInitializerParams.getKey(i),
          convertFacetParam(protoFacetHandlerInitializerParams.getValue(i)));
    }
    return result;
  }

  private SenseiProtos.FacetHandlerInitializerParam convertFacetParam(FacetHandlerInitializerParam facetHandlerInitializerParam) {
    SenseiProtos.FacetHandlerInitializerParam.Builder protoParams = SenseiProtos.FacetHandlerInitializerParam.newBuilder();

    for (String paramName : facetHandlerInitializerParam.getBooleanParamNames()) {
      SenseiProtos.BooleanParams.Builder paramBuilder = SenseiProtos.BooleanParams.newBuilder();
      paramBuilder.setKey(paramName);
      boolean[] param = facetHandlerInitializerParam.getBooleanParam(paramName);
      if(param != null) {
        for (int i = 0; i < param.length; i++) {
          paramBuilder.addValue(param[i]);
        }
      } else {
        paramBuilder.setIsNull(true);
      }
      protoParams.addBooleanParam(paramBuilder.build());
    }

    for (String paramName : facetHandlerInitializerParam.getIntParamNames()) {
      SenseiProtos.IntParams.Builder paramBuilder = SenseiProtos.IntParams.newBuilder();
      paramBuilder.setKey(paramName);
      int[] param = facetHandlerInitializerParam.getIntParam(paramName);
      if (param != null) {
        for (int i = 0; i < param.length; i++) {
          paramBuilder.addValue(param[i]);
        }
      } else {
        paramBuilder.setIsNull(true);
      }

      protoParams.addIntParam(paramBuilder.build());
    }

    for (String paramName : facetHandlerInitializerParam.getLongParamNames()) {
      SenseiProtos.LongParams.Builder paramBuilder = SenseiProtos.LongParams.newBuilder();
      paramBuilder.setKey(paramName);
      long[] param = facetHandlerInitializerParam.getLongParam(paramName);
      if (param != null) {
        for (int i = 0; i < param.length; i++) {
          paramBuilder.addValue(param[i]);
        }
      } else {
        paramBuilder.setIsNull(true);
      }
      protoParams.addLongParam(paramBuilder.build());
    }

    for (String paramName : facetHandlerInitializerParam.getBooleanParamNames()) {
      SenseiProtos.StringParams.Builder paramBuilder = SenseiProtos.StringParams.newBuilder();
      paramBuilder.setKey(paramName);
      List<String> param = facetHandlerInitializerParam.getStringParam(paramName);
      if (param != null) {
        for (int i = 0; i < param.size(); i++) {
          paramBuilder.addValue(param.get(i));
        }
      } else {
        paramBuilder.setIsNull(true);
      }

      protoParams.addStringParam(paramBuilder.build());
    }

    for (String paramName : facetHandlerInitializerParam.getDoubleParamNames()) {
      SenseiProtos.DoubleParams.Builder paramBuilder = SenseiProtos.DoubleParams.newBuilder();
      paramBuilder.setKey(paramName);
      double[] param = facetHandlerInitializerParam.getDoubleParam(paramName);
      if (param != null) {
        for (int i = 0; i < param.length; i++) {
          paramBuilder.addValue(param[i]);
        }
      }  else {
        paramBuilder.setIsNull(true);
      }

      protoParams.addDoubleParam(paramBuilder.build());
    }

    for (String paramName : facetHandlerInitializerParam.getByteArrayParamNames()) {
      SenseiProtos.ByteArrayParams.Builder paramBuilder = SenseiProtos.ByteArrayParams.newBuilder();
      paramBuilder.setKey(paramName);
      byte[] param = facetHandlerInitializerParam.getByteArrayParam(paramName);
      if (param != null) {
        for (int i = 0; i < param.length; i++) {
          paramBuilder.setValue(ByteString.copyFrom(param));
        }
      } else {
        paramBuilder.setIsNull(true);
      }
      protoParams.addByteParam(paramBuilder.build());
    }

    return protoParams.build();
  }

  private FacetHandlerInitializerParam convertFacetParam(SenseiProtos.FacetHandlerInitializerParam protoFacetHandlerInitializerParam) {
    DefaultFacetHandlerInitializerParam facetHandlerInitializerParam = new DefaultFacetHandlerInitializerParam();

    for (SenseiProtos.BooleanParams param : protoFacetHandlerInitializerParam.getBooleanParamList()) {
      boolean[] value = null;
      if(!param.getIsNull()) {
        value = new boolean[param.getValueCount()];
        for (int i = 0; i < value.length; i++) {
          value[i] = param.getValue(i);
        }
      }
      facetHandlerInitializerParam.putBooleanParam(param.getKey(), value);
    }

    for (SenseiProtos.IntParams param : protoFacetHandlerInitializerParam.getIntParamList()) {
      int[] value = null;

      if (!param.getIsNull()) {
        value = new int[param.getValueCount()];
        for (int i = 0; i < value.length; i++) {
          value[i] = param.getValue(i);
        }
      }
      facetHandlerInitializerParam.putIntParam(param.getKey(), value);
    }

    for (SenseiProtos.LongParams param : protoFacetHandlerInitializerParam.getLongParamList()) {
      long[] value = null;
      if (!param.getIsNull()) {
        value = new long[param.getValueCount()];
        for (int i = 0; i < value.length; i++) {
          value[i] = param.getValue(i);
        }
      }
      facetHandlerInitializerParam.putLongParam(param.getKey(), value);
    }

    for (SenseiProtos.StringParams param : protoFacetHandlerInitializerParam.getStringParamList()) {
      List<String> value = null;
      if(!param.getIsNull()) {
        value = param.getValueList();
      }
      facetHandlerInitializerParam.putStringParam(param.getKey(), value);
    }

    for (SenseiProtos.DoubleParams param : protoFacetHandlerInitializerParam.getDoubleParamList()) {
      double[] value = null;
      if (!param.getIsNull()) {
        value = new double[param.getValueCount()];
        for (int i = 0; i < value.length; i++) {
          value[i] = param.getValue(i);
        }
      }
      facetHandlerInitializerParam.putDoubleParam(param.getKey(), value);
    }

    for (SenseiProtos.ByteArrayParams param : protoFacetHandlerInitializerParam.getByteParamList()) {
      ByteString value = null;
      if (!param.getIsNull()) {
        value = param.getValue();
      }
      facetHandlerInitializerParam.putByteArrayParam(param.getKey(), value.toByteArray());
    }

    return facetHandlerInitializerParam;
  }
}
