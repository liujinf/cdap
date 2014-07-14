/*
 * Copyright 2012-2014 Continuuity, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.continuuity.gateway.router;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.datafabric.ReactorDatasetNamespace;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.net.URI;

/**
 * We hide internal namespacing of datasets from user, so we want to namespace it here.
 */
public class DatasetsProxyRule implements ProxyRule {
  private final ReactorDatasetNamespace namespace;

  public DatasetsProxyRule(CConfiguration conf) {
    this.namespace = new ReactorDatasetNamespace(conf, DataSetAccessor.Namespace.USER);
  }

  @Override
  public HttpRequest apply(HttpRequest request) {
    String path = URI.create(request.getUri()).normalize().getPath();
    String[] uriParts = StringUtils.split(path, '/');
    if ((uriParts.length >= 4) && uriParts[1].equals("data") && uriParts[2].equals("datasets")) {
      // three parts with '/' wrapping them
      int insertAt = uriParts[0].length() + uriParts[1].length() + uriParts[2].length() + 4;
      String datasetName = uriParts[3];
      String newPath = path.substring(0, insertAt) + namespace.namespace(datasetName);
      if (insertAt + datasetName.length() < path.length()) {
        int copyAfter = insertAt + datasetName.length();
        newPath = newPath + path.substring(copyAfter);
      }
      request.setUri(newPath);
    }

    return request;
  }
}
