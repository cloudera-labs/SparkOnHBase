/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.hbase

import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.spark.Logging
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Class for management of credentials on single machine in cluster
 */
object Security extends Logging {

  val credentials = SparkHadoopUtil.get.getCurrentUserCredentials()
  @transient val ugi = UserGroupInformation.getCurrentUser
  //ugi.addCredentials(credentials) //add default credentials
  // specify that this is a proxy user
  ugi.setAuthenticationMethod(AuthenticationMethod.PROXY)

  /**
   * add specific credentials for the current user
   * e.g. Kerberos credentials obtained from master
   * @param cred credentials to add to the current user
   */
  def applyCreds(cred : Credentials) {
      ugi.addCredentials(cred)
  }

}
