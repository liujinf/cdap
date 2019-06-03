/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.namespace.runtimewiring;

import com.google.inject.Inject;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.store.namespace.datafetchers.NamespaceDataFetcher;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceFields;
import io.cdap.cdap.graphql.store.namespace.schema.NamespaceTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * NamespaceQuery type runtime wiring. Registers the data fetchers for the NamespaceQuery type.
 */
public class NamespaceQueryTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private final NamespaceDataFetcher namespaceDataFetcher;

  @Inject
  NamespaceQueryTypeRuntimeWiring(NamespaceDataFetcher namespaceDataFetcher) {
    this.namespaceDataFetcher = namespaceDataFetcher;
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(NamespaceTypes.NAMESPACE_QUERY)
      .dataFetcher(NamespaceFields.NAMESPACES, namespaceDataFetcher.getNamespacesDataFetcher())
      .dataFetcher(GraphQLFields.NAMESPACE, namespaceDataFetcher.getNamespaceFromQueryDataFetcher())
      .build();
  }

}
