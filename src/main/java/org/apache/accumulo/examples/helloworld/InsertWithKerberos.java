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
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.examples.cli.ClientOpts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts 10K rows (50K entries) into accumulo with each row having 5 entries.
 */
public class InsertWithKerberos {

  private static final Logger log = LoggerFactory.getLogger(InsertWithKerberos.class);

  public static void main(String[] args)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(InsertWithKerberos.class.getName(), args);
    UserGroupInformation ugi;
    final Properties props = opts.getClientProperties();
    final String principal = props.getProperty("auth.principal");
    final String keytabPath = props.getProperty("auth.token");
    final String qop = props.getProperty("sasl.qop");
    final String primary = props.getProperty("sasl.kerberos.server.primary");
    log.debug(String.format("qop=%s, primary=%s", qop, primary));

    AccumuloClient client;

    try {

      final String tableName = "hellotable";
      final Configuration hadoopConf = new Configuration(false);
      hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(hadoopConf);

      final File keytabFile = new File(keytabPath);

      ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
      // final KerberosToken ktoken = new KerberosToken(principal, keytabFile);

      client = ugi.doAs((PrivilegedExceptionAction<AccumuloClient>) () -> {
        // UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        final KerberosToken ktoken = new KerberosToken();

        log.debug("principal: " + ktoken.getPrincipal() + " is ready for Kerberos");
        AccumuloClient aclient = Accumulo.newClient().from(props).as(principal, ktoken).useSasl()
            .build();
        log.debug("AccumuloClient instance created with : " + aclient.whoami() + " for Kerberos");
        try {

          for (String table : aclient.tableOperations().list()) {
            log.info(String.format("table found : %s", table));
          }

          for (String user : aclient.securityOperations().listLocalUsers()) {
            log.info(String.format("user found : %s", user));
          }
        } catch (Exception e) {
          log.info("Failed to get table and user list ");
        }
        return aclient;
      });
      // client.tableOperations().create("hellotable");
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        NewTableConfiguration ntc = new NewTableConfiguration();
        Map<String,String> emptyMap = Collections.emptyMap();
        ntc.setTimeType(TimeType.MILLIS);
        // log.debug("getAuthenticationToken.getClass()==>"
        // + ((ClientContext) aclient).getAuthenticationToken().getClass().getName());
        try {
          if (!client.tableOperations().exists(tableName)) {
            client.tableOperations().create(tableName, ntc.setProperties(emptyMap));
          }
        } catch (Exception ex) {
          log.debug("Failed to create table");
          ex.printStackTrace();
          client.close();
          return null;
        }

        try (BatchWriter bw = client.createBatchWriter(tableName)) {
          log.trace("writing ...");
          for (int i = 0; i < 10000; i++) {
            Mutation m = new Mutation(String.format("row_%d", i));
            for (int j = 0; j < 5; j++) {
              m.put("colfam", String.format("colqual_%d", j),
                  new Value((String.format("value_%d_%d", i, j)).getBytes()));
            }
            bw.addMutation(m);
            if (i % 100 == 0) {
              log.trace(String.valueOf(i));
            }
          }
        } catch (Exception e) {
          log.debug(e.getMessage());
        }
        client.close();
        return null;
      });
    } catch (Exception e) {
      log.debug("Exception caught in Main....");
      log.debug(e.getMessage());
      return;
    }

  }

}
