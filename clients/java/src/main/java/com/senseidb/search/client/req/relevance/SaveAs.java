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

/**
 * "save as" part below is optional, if specified, this runtime generated model
 * will be saved with a name. After a runtime model is named, it will be
 * convenient to use next time, we can just specify the model name. Attn: Even
 * if we do not name a runtime model, the system will also automatically cache a
 * certain amount of anonymous runtime models, so there is no extra compilation
 * cost for second request with the same model function body and signature.
 * 
 */
public class SaveAs {
    private String name;
    private boolean overwrite;

    public SaveAs(String name, boolean overwrite) {
        super();
        this.name = name;
        this.overwrite = overwrite;
    }

    public String getName() {
        return name;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

}
