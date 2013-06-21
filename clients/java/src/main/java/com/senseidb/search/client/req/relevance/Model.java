/**
 * This software is licensed to you under the Apache License, Version 2.0 (the
 * "Apache License").
 *
 * LinkedIn's contributions are made under the Apache License. If you contribute
 * to the Software, the contributions will be deemed to have been made under the
 * Apache License, unless you expressly indicate otherwise. Please do not make any
 * contributions that would be inconsistent with the Apache License.
 *
 * You may obtain a copy of the Apache License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, this software
 * distributed under the Apache License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the Apache
 * License for the specific language governing permissions and limitations for the
 * software governed under the Apache License.
 *
 * © 2012 LinkedIn Corp. All Rights Reserved.  
 */

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
 * <li>variables — User provided variable, the variable name and type are
 * defined here, but the actual values has to be filled outside the model part,
 * but in the values part.
 * 
 * <li>facets — Define what facet/column will be used in the relevance model. It
 * automatically defined the variable name the same as the facet name.
 * 
 * <li>function_params — Define which parameters will be used in the function.
 * All the parameters listed here have to be defined either in the variables
 * part, or the facets part.
 * 
 * <li>function — The real function body. Java code here. It must have a return
 * type, and return a float value. No malicious class can be used, a custom
 * class loader will prevent it from being loaded if it is not in the white
 * class list.
 * <ol>
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
                List<String> variablesList = this.model.variables.get(type
                        .getValue());
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
