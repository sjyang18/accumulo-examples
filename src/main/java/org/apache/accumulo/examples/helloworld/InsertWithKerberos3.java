/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.examples.helloworld;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.*;
import org.apache.accumulo.examples.cli.ClientOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts 10K rows (50K entries) into accumulo with each row having 5 entries.
 */
public class InsertWithKerberos3 {

  private static final Logger log = LoggerFactory.getLogger(InsertWithKerberos3.class);

  public static void main(String[] args)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(InsertWithKerberos3.class.getName(), args);
    UserGroupInformation ugi;
    final Properties props = opts.getClientProperties();
    final String principal = props.getProperty("auth.principal");
    final String keytabPath = props.getProperty("auth.token");
    AccumuloClient client;

    try {
      final String tableName = "hellotable2";

      final Configuration hadoopConf = new Configuration(false);
      hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(hadoopConf);
      UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

      final File keytabFile = new File(keytabPath);
      AccumuloClient aclient = Accumulo.newClient().from(opts.getClientPropsPath()).useSasl()
          .build();
      NewTableConfiguration ntc = new NewTableConfiguration();
      Map<String,String> emptyMap = Collections.emptyMap();
      ntc.setTimeType(TimeType.MILLIS);
      final TableOperations tableOps = aclient.tableOperations();
      if (!tableOps.exists(tableName)) {
        tableOps.create(tableName, ntc.setProperties(emptyMap));
      }
      aclient.close();
    } catch (Exception e) {
      log.debug("Exception caught in Main....");
      e.printStackTrace();
      log.debug(e.getMessage());
    }
  }

}
