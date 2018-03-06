aws-ssm-config
==============

Config File Management using AWS SSM Shared Parameter Store, serialized to JSON, YAML, or Java properties files on hosts.

Installation
------------

Requires Java 8.

```
./gradlew distZip
unzip build/distributions/aws-ssm-config.zip -d <path to target directory>
```

Usage
-----

```
./bin/aws-ssm-config --profile myprofile --region us-east-1 -C /ep/conf \
    -f ep.properties \
    -f ep.override.properties \
    -s /ep/ecs/conf \
    -s /ep/ecs/conf/preprod \
    -s /ep/ecs/conf/admin \
    -s /ep/ecs/conf/preprod/admin
```

