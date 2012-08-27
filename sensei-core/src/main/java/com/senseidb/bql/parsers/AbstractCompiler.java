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
package com.senseidb.bql.parsers;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;
import org.json.JSONObject;

public abstract class AbstractCompiler {

  public AbstractCompiler()
  {
    super();
  }

  public abstract JSONObject compile(String expression) throws RecognitionException;
  public abstract String getErrorMessage(RecognitionException error);

  protected void printTree(CommonTree ast)
  {
    print(ast, 0);
  }

  private void print(CommonTree tree, int level)
  {
    // Indent level
    for (int i = 0; i < level; i++)
    {
      System.out.print("--");
    }

    if (tree == null)
    {
      System.out.println(" null tree.");
      return;
    }

    // Print node description: type code followed by token text
    System.out.println(" " + tree.getType() + " " + tree.getText());
    
    // Print all children
    if (tree.getChildren() != null)
    {
      for (Object ie : tree.getChildren())
      {
        print((CommonTree) ie, level + 1);
      }
    }
  }

}
