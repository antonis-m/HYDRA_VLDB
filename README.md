## Hydra Sketch

#### Requirements

* maven
* spark
* aws-cli
* access to aws S3 buckets

#### Compile

```bash
mvn package -DskipTests
```

#### Run

```bash
spark-submit --master yarn --deploy-mode client --class "com.github.xxxxlab.hydrasketch.SparkHydra" target/hydrasketch-solver-0.1.1.jar "/home/hadoop/submitcode/HYDRA/config/exp.conf"
```

`config/config.conf` contains parameters for Hydra Sketch.

The path in the command and exp.conf need to be changed accordingly.
