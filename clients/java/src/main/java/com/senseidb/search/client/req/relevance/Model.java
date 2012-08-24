package com.senseidb.search.client.req.relevance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.senseidb.search.client.json.JsonField;
import com.senseidb.search.client.req.FacetType;

/**
 * The relevance json has two parts, one is the model part, another is the
 * values part. Model part should be relatively static. The values part provides
 * the input required by the static model. Each request may have different
 * values part, but may probably use the same model.
 * 
 * Inside the model part, we define 4 items:
 * 
 * <ol>
 * <li>variables — User provided variable, the variable name and type are defined
 * here, but the actual values has to be filled outside the model part, but in
 * the values part.
 * 
 * <li>facets — Define what facet/column will be used in the relevance model. It
 * automatically defined the variable name the same as the facet name.
 *
 * <li>function_params — Define which parameters will be used in the function. All
 * the parameters listed here have to be defined either in the variables part,
 * or the facets part.
 *
 * <li>function — The real function body. Java code here. It must have a return
 * type, and return a float value. No malicious class can be used, a custom
 * class loader will prevent it from being loaded if it is not in the white
 * class list.
 *  <ol>
 */
public class Model {
  private Map<String, List<String>> variables = new HashMap<String, List<String>>();;
  private Map<String, List<String>> facets = new HashMap<String, List<String>>();
  @JsonField("function_params")
  private List<String> functionParams = new ArrayList<String>();
  private String function;
  @JsonField("save_as")
  private SaveAs saveAs;

  public static ModelBuilder builder() {
    return new ModelBuilder();
  }

  public static class ModelBuilder {
    private final Model model;

    public ModelBuilder() {
      this.model = new Model();
    }

    public ModelBuilder function(String function) {
      this.model.function = function;
      return this;
    }

    public ModelBuilder addFunctionParams(String... params) {
      if (params != null) {
        this.model.functionParams.addAll(Arrays.asList(params));
      }
      return this;
    }

    public ModelBuilder addFacets(RelevanceFacetType type, String... names) {
      if (names != null) {
        List<String> facets = this.model.facets.get(type.getValue());
        if (facets == null) {
          facets = new ArrayList<String>();
          this.model.facets.put(type.getValue(), facets);
        }
        facets.addAll(Arrays.asList(names));
      }
      return this;
    }

    public ModelBuilder addVariables(VariableType type, String... variables) {
      if (variables != null) {
        List<String> variablesList = this.model.variables.get(type.getValue());
        if (variablesList == null) {
          variablesList = new ArrayList<String>();
          this.model.variables.put(type.getValue(), variablesList);
        }
        variablesList.addAll(Arrays.asList(variables));
      }
      return this;
    }

    public ModelBuilder saveAs(String name, boolean overwrite) {
      this.model.saveAs = new SaveAs(name, overwrite);
      return this;
    }

    public Model build() {
      return model;
    }
  }

}
