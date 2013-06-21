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

package com.senseidb.test.client;

import java.util.Arrays;
import java.util.Collections;

import com.senseidb.search.client.SenseiServiceProxy;
import com.senseidb.search.client.json.JsonSerializer;
import com.senseidb.search.client.req.Operator;
import com.senseidb.search.client.req.Selection;
import com.senseidb.search.client.req.SenseiClientRequest;
import com.senseidb.search.client.res.SenseiResult;

public class SendRawQuery {
    public static void main(String[] args) throws Exception {
        //String request = new String(IOUtils.getBytes(SendRawQuery.class.getClassLoader().getResourceAsStream("json/car-query.json")), "UTF-8");
        SenseiClientRequest senseiRequest = SenseiClientRequest.builder()
                .paging(10, 0)
                .fetchStored(true)
                .addSelection(Selection.terms("color", Arrays.asList("red", "blue"), Collections.EMPTY_LIST, Operator.or))
                .build();
        String requestStr = JsonSerializer.serialize(senseiRequest).toString();
        System.out.println(requestStr);
        SenseiResult senseiResult = new SenseiServiceProxy("localhost", 8081).sendBQL(
            "SELECT *");
        System.out.println(senseiResult.toString());
    }
}
