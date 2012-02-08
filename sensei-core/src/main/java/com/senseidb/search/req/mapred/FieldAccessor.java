package com.senseidb.search.req.mapred;

public interface FieldAccessor {
    public Object get(String fieldName, int docId);
    public String getString(String fieldName, int docId);
    public long getLong(String fieldName, int docId);
    public double getDouble(String fieldName, int docId);
    public short getShort(String fieldName, int docId);
    public int getInteger(String fieldName, int docId);
    public float getFloat(String fieldName, int docId);
    public Object[] getArray(String fieldName, int docId);
    public Object getByUID(String fieldName, long uid);
    public String getStringByUID(String fieldName, long uid);
    public long getLongByUID(String fieldName, long uid);
    public double getDoubleByUID(String fieldName, long uid);
    public short getShortByUID(String fieldName, long uid);
    public int getIntegerByUID(String fieldName, long uid);
    public float getFloatByUID(String fieldName, long uid);
    public Object[] getArrayByUID(String fieldName, long uid); 
}
