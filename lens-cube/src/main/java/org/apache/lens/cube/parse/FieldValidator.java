/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.cube.parse;

import java.util.*;

import org.apache.lens.cube.metadata.CubeInterface;
import org.apache.lens.cube.metadata.DerivedCube;
import org.apache.lens.cube.metadata.JoinChain;
import org.apache.lens.cube.metadata.ReferencedDimAtrribute;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Validate fields based on cube queryability
 */
public class FieldValidator implements ContextRewriter {

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    validateFields(cubeql);
  }

  public void validateFields(CubeQueryContext cubeql) throws SemanticException {
    CubeInterface cube = cubeql.getCube();
    if (cube == null) {
      return;
    }

    if (!cube.allFieldsQueriable()) {
      // do queried field validation
      List<DerivedCube> dcubes;
      try {
        dcubes = cubeql.getMetastoreClient().getAllDerivedQueryableCubes(cube);
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      // dim attributes and chained source columns should only come from WHERE and GROUP BY ASTs
      Set<String> queriedDimAttrs = new LinkedHashSet<String>();
      Set<String> queriedMsrs = new LinkedHashSet<String>(cubeql.getQueriedMsrs());
      Set<String> chainedSrcColumns = new HashSet<String>();

      findDimAttrsAndChainSourceColumns(cubeql, cubeql.getGroupByAST(), queriedDimAttrs, chainedSrcColumns);
      findDimAttrsAndChainSourceColumns(cubeql, cubeql.getWhereAST(), queriedDimAttrs, chainedSrcColumns);

      // remove chained ref columns from field validation
      Iterator<String> iter = queriedDimAttrs.iterator();

      while (iter.hasNext()) {
        String attr = iter.next();
        if (cube.getDimAttributeByName(attr) instanceof ReferencedDimAtrribute
          && ((ReferencedDimAtrribute) cube.getDimAttributeByName(attr)).isChainedColumn()) {
          iter.remove();
          ReferencedDimAtrribute rdim = (ReferencedDimAtrribute)cube.getDimAttributeByName(attr);
          chainedSrcColumns.addAll(cube.getChainByName(rdim.getChainName()).getSourceColumns());
        }
      }

      // do validation
      // Find atleast one derived cube which contains all the dimensions
      // queried.
      boolean derivedCubeFound = false;
      for (DerivedCube dcube : dcubes) {
        if (dcube.getDimAttributeNames().containsAll(chainedSrcColumns)
          && dcube.getDimAttributeNames().containsAll(queriedDimAttrs)) {
          // remove all the measures that are covered
          queriedMsrs.removeAll(dcube.getMeasureNames());
          derivedCubeFound = true;
        }
      }
      Set<String> nonQueryableFields = getNonQueryableAttributes(cubeql);
      if (!derivedCubeFound && !nonQueryableFields.isEmpty()) {
        throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, nonQueryableFields.toString());
      }
      if (!queriedMsrs.isEmpty()) {
        // Add appropriate message to know which fields are not queryable together
        if (!nonQueryableFields.isEmpty()) {
          throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, nonQueryableFields.toString() + " and "
            + queriedMsrs.toString());
        } else {
          throw new SemanticException(ErrorMsg.FIELDS_NOT_QUERYABLE, queriedMsrs.toString());
        }
      }
    }
  }

  private Set<String> getNonQueryableAttributes(CubeQueryContext cubeql) {
    Set<String> nonQueryableFields = new LinkedHashSet<String>();
    nonQueryableFields.addAll(cubeql.getQueriedDimAttrs());
    for (String joinChainAlias : cubeql.getJoinchains().keySet()) {
      if (cubeql.getColumnsQueried(joinChainAlias) != null) {
        for (String chaincol : cubeql.getColumnsQueried(joinChainAlias)) {
          nonQueryableFields.add(joinChainAlias + "." + chaincol);
        }
      }
    }
    return nonQueryableFields;
  }


  private void findDimAttrsAndChainSourceColumns(final CubeQueryContext cubeql,
                                                 final ASTNode tree,
                                                 final Set<String> dimAttributes,
                                                 final Set<String> chainSourceColumns) throws SemanticException {
    if (tree == null) {
      return;
    }

    final boolean hasCube = cubeql.hasCubeInQuery();
    final CubeInterface cube = cubeql.getCube();

    HQLParser.bft(tree, new HQLParser.ASTNodeVisitor() {
      @Override
      public void visit(HQLParser.TreeNode treeNode) throws SemanticException {
        if (!hasCube) return;

        ASTNode astNode = treeNode.getNode();
        if (astNode.getToken().getType() == HiveParser.DOT) {
          // At this point alias replacer has run, so all columns are of the type table.column name
          ASTNode aliasNode = HQLParser.findNodeByPath((ASTNode) astNode.getChild(0), HiveParser.Identifier);
          String tabName = aliasNode.getText().toLowerCase().trim();
          ASTNode colNode = (ASTNode) astNode.getChild(1);
          String colName = colNode.getText().toLowerCase().trim();

          // Check if table is a join chain
          if (cubeql.getJoinchains().containsKey(tabName)) {
            // this 'tabName' is a join chain, so add all source columns
            chainSourceColumns.addAll(cubeql.getJoinchains().get(tabName).getSourceColumns());
          }
          // Alternatively, check if this is a dimension attribute
          else if (tabName.equalsIgnoreCase(cubeql.getAliasForTabName(cube.getName()))
            && cube.getDimAttributeNames().contains(colName)) {

            // If this is a referenced dim attribute leading to a chain, then instead of adding this
            // column, we add the source columns of the chain.
            if (cube.getDimAttributeByName(colName) instanceof ReferencedDimAtrribute
              && ((ReferencedDimAtrribute) cube.getDimAttributeByName(colName)).isChainedColumn()) {
              ReferencedDimAtrribute rdim = (ReferencedDimAtrribute) cube.getDimAttributeByName(colName);
              chainSourceColumns.addAll(cube.getChainByName(rdim.getChainName()).getSourceColumns());
            } else {
              // This is a dim attribute, needs to be validated
              dimAttributes.add(colName);
            }
          }
        }
      }
    });

  }

}
