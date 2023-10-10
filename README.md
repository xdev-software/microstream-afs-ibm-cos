[![Latest version](https://img.shields.io/maven-central/v/com.xdev-software/microstream-afs-ibm-cos?logo=apache%20maven)](https://mvnrepository.com/artifact/com.xdev-software/microstream-afs-ibm-cos)
[![Build](https://img.shields.io/github/actions/workflow/status/xdev-software/microstream-afs-ibm-cos/checkBuild.yml?branch=develop)](https://github.com/xdev-software/microstream-afs-ibm-cos/actions/workflows/checkBuild.yml?query=branch%3Adevelop)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=xdev-software_microstream-afs-ibm-cos&metric=alert_status)](https://sonarcloud.io/dashboard?id=xdev-software_microstream-afs-ibm-cos)

# microstream-afs-ibm-cos

A connector for [MicroStream](https://microstream.one/) which allows storing data in
the [IBM Cloud Object Storage](https://www.ibm.com/cloud/object-storage).

It uses the [IBM-provided Java SDK](https://github.com/IBM/ibm-cos-sdk-java).

The connector works virtually identical to
the [AWS S3 Connector](https://docs.microstream.one/manual/storage/storage-targets/blob-stores/aws-s3.html) of
MicroStream
but for IBM COS instead of AWS S3.

## Installation

[Installation guide for the latest release](https://github.com/xdev-software/microstream-afs-ibm-cos/releases/latest#Installation)

## Supported MicroStream versions

The connector supports
[version 08.01.01-MS-GA of the MicroStream](https://central.sonatype.dev/artifact/one.microstream/microstream-storage/08.01.01-MS-Ghttps://central.sonatype.dev/artifact/one.microstream/microstream-storage/08.01.01-MS-GA).

If you are using a different, not listed version of MicroStream, this shouldn't be a problem.
Usually you can simply exclude the dependent version of MicroStream.

## Support

If you need support as soon as possible and you can't wait for any pull request, feel free to
use [our support](https://xdev.software/en/services/support).

## Contributing

See the [contributing guide](./CONTRIBUTING.md) for detailed instructions on how to get started with our project.

## Dependencies and Licenses

View the [license of the current project](LICENSE) or
the [summary including all dependencies](https://xdev-software.github.io/microstream-afs-ibm-cos/dependencies/)
