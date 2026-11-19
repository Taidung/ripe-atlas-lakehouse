# Image Spark của đồ án: nhúng sẵn jar S3A (hadoop-aws) để job chạy được offline,
# thay cho spark.jars.packages vốn tải từ Maven Central mỗi khi container được tạo lại.
FROM tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460

# Jar đặt ở /opt/spark/extra-jars và nạp qua spark.jars (user classpath), KHÔNG chép vào
# /opt/spark/jars: image đã có Jackson 2.15.2, đặt Jackson 2.12 cạnh nó sẽ gây xung đột.
# Danh sách và SHA-256 lấy từ đúng bộ jar Ivy đã resolve và chạy được ở giai đoạn 1.
# Tải từ Maven Central, nếu lỗi thì thử mirror Maven Central của Google. SHA-256 được pin nên bytes nhận về đúng
# như nhau bất kể nguồn. (CI 2026-11-19: runner GitHub nhận lỗi HTTP từ repo1.maven.org, curl exit 22, trong khi
# máy local tải bình thường; --retry mặc định không retry lỗi 4xx nên thêm --retry-all-errors.)
ARG MAVEN=https://repo1.maven.org/maven2
ARG MAVEN_MIRROR=https://maven-central.storage-download.googleapis.com/maven2
RUN set -eu; mkdir -p /opt/spark/extra-jars; cd /opt/spark/extra-jars; \
  fetch() { \
    f="$(basename "$1")"; \
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 3 -o "$f" "$MAVEN/$1" \
      || { echo "primary failed: $MAVEN/$1, trying mirror" >&2; curl -fsSL --retry 5 --retry-all-errors --retry-delay 3 -o "$f" "$MAVEN_MIRROR/$1"; } \
      || { echo "FAILED to download $1 from both sources" >&2; exit 1; }; \
    echo "$2  $f" | sha256sum -c -; \
  }; \
  fetch org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar 53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a; \
  fetch com/amazonaws/aws-java-sdk-core/1.12.262/aws-java-sdk-core-1.12.262.jar 06fed99f929c389e168278c1b111aaf1afb270d07202fbbce92e73d1e4279678; \
  fetch com/amazonaws/aws-java-sdk-s3/1.12.262/aws-java-sdk-s3-1.12.262.jar a0cb7580178bc0d4d3558295a5bb890008478da8a6b3241989c71238b0ed3be5; \
  fetch com/amazonaws/aws-java-sdk-sts/1.12.262/aws-java-sdk-sts-1.12.262.jar 659f48d8ebc02025c5042cc4b925f62610e81f334498f1b9e7a271ce20ecbcae; \
  fetch com/amazonaws/aws-java-sdk-dynamodb/1.12.262/aws-java-sdk-dynamodb-1.12.262.jar 7af4ba06d0d9b3c2ab435089caecd0567810d203aa285f7aa72861f1964d442e; \
  fetch com/amazonaws/aws-java-sdk-kms/1.12.262/aws-java-sdk-kms-1.12.262.jar c00f5ae7b334255f661d85512e7203af9523a04fb65d0c431c2eea2003ed8ae2; \
  fetch com/amazonaws/jmespath-java/1.12.262/jmespath-java-1.12.262.jar 9001ea993a45d82a2d3086edcbda8a40b4e012d049a70fbf071fd329c0fbd777; \
  fetch com/fasterxml/jackson/core/jackson-annotations/2.12.6/jackson-annotations-2.12.6.jar ddf46e401a7d9ea3b481c263fa192285d13c50982a5882b22f806639b9645ee4; \
  fetch com/fasterxml/jackson/core/jackson-core/2.12.6/jackson-core-2.12.6.jar 0026cff293bdba389fbbbc67a20fdd5f73e091554ab46671efa654c25c807ee6; \
  fetch com/fasterxml/jackson/core/jackson-databind/2.12.6.1/jackson-databind-2.12.6.1.jar eb60e494ba8c23e653da4db8e29af0342927fc7e1c60501bf99e93145738c696; \
  fetch org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar 9f7058778978ec2ad90c7224401a4b0013d93e0dd3fa0f72b25244ffa3507ca8

# pytest để chạy test Spark trong container (PySpark 3.5.5 có sẵn trong image).
# GX Core 1.25.0: đã kiểm tra cài cùng pandas 2.2.3 / numpy 1.26.4 / pyarrow 17.0.0 không đổi phiên bản.
RUN pip install --no-cache-dir pytest==8.3.5 great_expectations==1.25.0 prometheus-client==0.26.0

# Giữ PYTHONPATH của image gốc (pyspark + py4j), thêm package lakehouse của đồ án.
# Package `lakehouse` (transformation/) và `dq` (quality/) được mount ở /opt/app (xem docker-compose.yaml).
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app/transformation:/opt/app/quality
