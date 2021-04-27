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
import java.security.PrivilegedExceptionAction;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.examples.cli.ClientOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads all data between two rows
 */
public class ReadWithKerberos {

  private static final Logger log = LoggerFactory.getLogger(Read.class);

  public static void main(String[] args) throws TableNotFoundException {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(Read.class.getName(), args);
    UserGroupInformation ugi;
    final Properties props = opts.getClientProperties();
    final String principal = props.getProperty("auth.principal");
    final String keytabPath = props.getProperty("auth.token");
    final String qop = props.getProperty("sasl.qop");
    final String primary = props.getProperty("sasl.kerberos.server.primary");
    log.debug(String.format("qop=%s, primary=%s", qop, primary));

    try {
      final String tableName = "hellotable";
      final Configuration hadoopConf = new Configuration(false);
      hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(hadoopConf);

      final File keytabFile = new File(keytabPath);

      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        final KerberosToken ktoken = new KerberosToken();

        log.debug("principal: " + ktoken.getPrincipal() + " is ready for Kerberos");
        AccumuloClient aclient = Accumulo.newClient().from(props).as(principal, ktoken).useSasl()
            .build();
        log.debug("AccumuloClient instance created with : " + aclient.whoami() + " for Kerberos");
        if (!aclient.tableOperations().exists(tableName)) {
          aclient.tableOperations().create(tableName);
        }
        try (Scanner scan = aclient.createScanner(tableName, Authorizations.EMPTY)) {
          scan.setRange(new Range(new Key("row_0"), new Key("row_1002")));
          for (Entry<Key,Value> e : scan) {
            Key key = e.getKey();
            log.debug(key.getRow() + " " + key.getColumnFamily() + " " + key.getColumnQualifier()
                + " " + e.getValue());
          }
        }
        aclient.close();
        return null;

      });

    } catch (Exception e) {
      log.debug(e.getMessage());
      e.printStackTrace();
    }
  }
}
