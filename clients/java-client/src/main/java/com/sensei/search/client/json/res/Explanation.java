package com.sensei.search.client.json.res;

import java.util.ArrayList;
import java.util.List;

public class Explanation {
  Double value;
  String description;
  List<Explanation> details = new ArrayList<Explanation>();
  @Override
  public String toString() {
    return "Explanation [value=" + value + ", description=" + description + ", details=" + details + "]";
  }
}
