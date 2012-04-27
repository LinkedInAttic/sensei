package com.senseidb.search.client.res;

public class FieldValue {

  private String fieldName;
  private String fieldValues;

  public FieldValue(String fieldName, String fieldValues) {
    super();
    this.fieldName = fieldName;
    this.fieldValues = fieldValues;
  }
  public String getFieldName() {
    return fieldName;
  }
  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }
  public String getFieldValues() {
    return fieldValues;
  }
  public void setFieldValues(String fieldValues) {
    this.fieldValues = fieldValues;
  }
  @Override
  public String toString() {
    return "FieldValue [fieldName=" + fieldName + ", fieldValues=" + fieldValues + "]";
  }
}
