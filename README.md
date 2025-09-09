# Hazelcast JDBC Driver
Hazelcast JDBC Driver allows Java applications to connect to Hazelcast using the standard JDBC API.

## Supported Hazelcast version

#### Hazelcast 5
The JDBC driver version 5.x is compatible with Hazelcast version 5.x. For Hazelcast 4.2, use the JDBC Driver 4.2.

## Download the Driver

### Binaries

The driver comes in two packages:
1. `hazelcast-jdbc-{version}.jar` (open source)
2. `hazelcast-jdbc-enterprise-{version}.jar` (enterprise)

The open source one isn't able to connect to Hazelcast EE clusters, but has an
open source licence. The latter one uses proprietary licence, and can be used
for connecting to both OS and EE clusters.

Download directly the JAR file from [Releases](https://github.com/hazelcast/hazelcast-jdbc/releases) page (open source) or [Hazelcast repository](https://repository.hazelcast.com/release/com/hazelcast/hazelcast-jdbc-enterprise/) (enterprise).

### Maven Central

#### Stable versions

Open source:
```xml
<dependency>
    <groupId>com.hazelcast</groupId>
    <artifactId>hazelcast-jdbc</artifactId>
    <version>5.5.0</version>
</dependency>
```

Enterprise (located in Hazelcast Maven repository):
```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast</groupId>
        <artifactId>hazelcast-jdbc-enterprise</artifactId>
        <version>5.5.0</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <id>hazelcast-release</id>
        <name>Hazelcast Repository</name>
        <url>https://repository.hazelcast.com/release/</url>
        <releases>
            <enabled>true</enabled>
        </releases>
        <snapshots>
            <enabled>false</enabled>
        </snapshots>
    </repository>
</repositories>
```

#### Snapshot versions
To download the latest snapshot build you need to add
the dependency and also the snapshot repository.

Open source:
```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast</groupId>
        <artifactId>hazelcast-jdbc</artifactId>
        <version>5.6.0-SNAPSHOT</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <id>snapshot-repository</id>
        <name>Maven Snapshot Repository</name>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

Enterprise:
```xml
<dependencies>
    <dependency>
        <groupId>com.hazelcast</groupId>
        <artifactId>hazelcast-jdbc-enterprise</artifactId>
        <version>5.6.0-SNAPSHOT</version>
    </dependency>
</dependencies>

<repositories>
    <repository>
        <id>hazelcast-snapshots</id>
        <name>Hazelcast Snapshot Repository</name>
        <url>https://repository.hazelcast.com/snapshot/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>
```

## Connection URL

The URL must have the following structure, parts in `[]` are optional:
```
jdbc:hazelcast://host[:port][,host[:port]...]/[?property1=value1[&property2=value2]...]
```

To connect to a Viridian Serverless cluster, use:
```
jdbc:hazelcast://<cluster-id>/?discoveryToken=<yourDiscoveryToken>&cloudUrl=https://api.viridian.hazelcast.com&sslEnabled=true
```
Note: Viridian Serverless clusters require [additional client configuration for TLS](https://github.com/hazelcast/hazelcast-jdbc#hazelcast-cloud-configuration).


To connect to the legacy Cloud service, use:
```
jdbc:hazelcast://cluster-id/discoveryToken=value[&property1=value1[&property2=value2]...]
```

where:
* **jdbc:hazelcast:**: (Required) is a sub-protocol and is a constant.
* **host**: (Required) server address (or addresses separated with comma) to connect to, or the `cluster id` if the cluster is
in Hazelcast Viridian/Cloud (in which case you can find it in the Connect Client - Advanced Setup dialog).
* **port**: (Optional) Server port. Defaults to 5701.
* **propertyN**: (Optional) List of connection properties in the key-value form.

### Connection properties
The following list contains the properties supported by the `Hazelcast JDBC Driver`.

#### Common properties

| Property       | Type    | Description                     |
|----------------| ------- |---------------------------------|
| user           | String  | Hazelcast cluster username      |
| password       | String  | Hazelcast cluster password      |
| clusterName    | String  | Hazelcast cluster name          |
| discoveryToken | String  | Hazelcast Cloud discovery token |
| cloudUrl       | String  | Hazelcast Cloud URL             |

#### SSL properties
| Property                | Type    | Description                                                              |
|-------------------------| ------- |--------------------------------------------------------------------------|
| sslEnabled              | Boolean | Enable SSL for client connection                                         |
| trustStore              | String  | Path to truststore file. Alias for `trustCertCollectionFile`             |
| trustCertCollectionFile | String  | Path to truststore file. Alias for `trustStore`                          |
| trustStorePassword      | String  | Password to unlock the truststore file                                   |
| keyStore                | String  | Path to your keystore file                                               |
| keyFile                 | String  | Path to your keystore file                                               |
| keyStorePassword        | String  | Password to access the key from your keystore file                       |
| protocol                | String  | Name of the algorithm which is used in your TLS/SSL; default to `TLS`    |
| keyCertChainFile        | String  | Path to an X.509 certificate chain file in PEM format                    |
| factoryClassName        | String  | Fully qualified class name for the implementation of `SSLContextFactory` |
| javax.net.ssl.*         | String  | Properties starting with this prefix are passed to `SSLConfig` directly. |


#### GCP properties
| Property                       | Type    | Description   |
| ------------------------------ | ------- | ------------- |
| gcpPrivateKeyPath              | String  | A filesystem path to the private key for GCP service account in the JSON format; if not set, the access token is fetched from the GCP VM instance |
| gcpHzPort                      | String  | A range of ports where the plugin looks for Hazelcast members; if not set, the default value 5701-5708 is used |
| gcpProjects                    | String  | A list of projects where the plugin looks for instances; if not set, the current project is used |
| gcpRegion                      | String  | A region where the plugin looks for instances; if not set, the zones property is used; if it and zones property not set, all zones of the current region are used |
| gcpLabel                       | String  | A filter to look only for instances labeled as specified; property format: `key=value` |
| gcpUsePublicIp                 | Boolean | Use public IP Address |

#### AWS properties
| Property                       | Type    | Description   |
| ------------------------------ | ------- | ------------- |
| awsAccessKey                   | String  | Access key of your AWS account; if not set, iam-role is used |
| awsSecretKey                   | String  | Secret key of your AWS account; if not set, iam-role is used |
| awsIamRole                     | String  | IAM Role attached to EC2 instance used to fetch credentials (if `awsAccessKey`/`awsSecretKey` not specified); if not set, default IAM Role attached to EC2 instance is used |
| awsTagKey/awsTagValue          | String  | Filter to look only for EC2 Instances with the given key/value; multi values supported if comma-separated (e.g. `KeyA,KeyB`); comma-separated values behaves as AND conditions |
| awsRegion                      | String  | Region where Hazelcast members are running; default is the current region |
| awsHostHeader                  | String  | `ec2`, `ecs`, or the URL of a EC2/ECS API endpoint; automatically detected by default |
| awsSecurityGroupName           | String  | Filter to look only for EC2 instances with the given security group |
| awsConnectionTimeoutSeconds    | Integer | Connection timeout when making a call to AWS API; default to `10` |
| awsReadTimeoutSeconds          | Integer | Read timeout when making a call to AWS API; default to `10` |
| awsConnectionRetries           | Integer | Number of retries while connecting to AWS API; default to 3 |
| awsHzPort                      | String  | A range of ports where the plugin looks for Hazelcast members; default is 5701-5708 |
| awsUsePublicIp                 | Boolean | Use public IP Address |

#### Azure properties
| Property                       | Type    | Description   |
| ------------------------------ | ------- | ------------- |
| azureInstanceMetadataAvailable | Boolean | This property must be configured as `false` in order to be able to use the Azure properties. It is `true` by default. |
| azureClientId                  | String  | Azure Active Directory Service Principal client ID |
| azureClientSecret              | String  | Azure Active Directory Service Principal client secret |
| azureTenantId                  | String  | Azure Active Directory tenant ID |
| azureSubscriptionId            | String  | Azure subscription ID |
| azureResourceGroup             | String  | Name of Azure resource group which the Hazelcast instance is running in |
| azureScaleSet                  | String  | name of Azure VM scale set. If this setting is configured, the plugin will search for instances over the resources only within this scale set |
| azureUsePublicIp               | Boolean | Use public IP Address |

#### Kubernetes properties
| Property                       | Type    | Description   |
| ------------------------------ | ------- | ------------- |
| k8sServiceDns                  | String  | Service DNS, usually in the form of `SERVICE-NAME.NAMESPACE.svc.cluster.local` |
| k8sServiceDnsTimeout           | Integer | Custom time for how long the DNS lookup is valid |
| k8sNamespace                   | String  | Kubernetes namespace where Hazelcast is running |
| k8sServiceName                 | String  | Service name used to scan only PODs connected to the given service; if not specified, then all PODs in the namespace are checked |
| k8sServicePort                 | Integer | Endpoint port of the service; if specified with a value greater than 0, it overrides the default; 0 by default |

#### Miscellaneous properties
| Property                       | Type    | Description                                                                                                                                                                             |
| ------------------------------ | ------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| smartRouting                   | Boolean | If `false`, the client will have only one connection to the cluster. Useful, if there are many clients, and we want to avoid each of them connecting to each member. `true` by default. |
| resubmissionMode               | String  | Strategy to retry failed queries. Valid values are: `NEVER` (the default), `RETRY_SELECTS`, `RETRY_SELECTS_ALLOW_DUPLICATES` and `RETRY_ALL`.                                           |

#### Hazelcast Cloud Configuration
For connecting to the Hazelcast cloud you only need to specify `discoveryToken` property and use the `cluster-id` as a host in
the URL: `jdbc:hazelcast://<cluster-id>/?discoveryToken=<yourDiscoveryToken>`.

If you're connecting to a [Viridian Serverless cluster](https://viridian.hazelcast.com/), also specify `cloudUrl=https://api.viridian.hazelcast.com` and `sslEnabled=true`: `jdbc:hazelcast://<cluster-id>/?discoveryToken=<yourDiscoveryToken>&cloudUrl=https://api.viridian.hazelcast.com&sslEnabled=true`. 

TLS is mandatory for Viridian Serverless cluster connections so pass TLS properties such as truststore, keystore etc along to `DriverManager.getConnection()`.

#### Additional Configuration
Besides URL, it is possible to use [Configuration Files](https://docs.hazelcast.com/hazelcast/latest/configuration/configuring-declaratively) and  [Overriding Configuration](https://docs.hazelcast.com/hazelcast/latest/configuration/configuring-declaratively#overriding-configuration-with-system-properties-and-environment-variables) to configure the [Hazelcast Java Client](https://docs.hazelcast.com/hazelcast/latest/clients/java)

## SQL
SQL support is in active development. Have a look at [SQL docs](https://docs.hazelcast.com/hazelcast/latest/sql/sql-overview).
