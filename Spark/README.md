# Spark

## Setup Spark using Ansible
Run Ansible script for HDFS, then run Ansible for Spark.
Ansible playbook:
- downloads and configure Spark 3.5.0
- sets environmental variables in `.bashrc`
- create a directory for eventLog in HDFS
- start Spark History Server

Remember to edit `inventory.ini` to set proper values of IP addresses.

To run Ansible, run the following command from `ansible` directory
```bash
ansible-playbook -i inventory.ini spark_setup.yml --key-file "~/.ssh/<private-key>"
```
Then you can run the Spark programs from a master node inside the cluster.

## Run Spark
1. To run a Spark job, you have to build a jar file using `sbt`:
    ```bash
    sbt package
    ```
2. Copy file jar file to Master Node using `scp`
   ```bash
   scp -i ~/.ssh/<private-key> ./Spark/<project-name>/target/scala-2.13/<project-name>_2.13-1.0.jar ubuntu@<master-public-ip>:~
   ```
3. Run Spark job using `spark-submit`
   ```bash
   spark-submit \
   --class de.tum.ddm.<main-class> \
   --num-executors 8 \    # optional argument to override default configuration
   --driver-memory 3g \    # optional argument to override default configuration
   --executor-memory 3g \  # optional argument to override default configuration
   --executor-cores 2 \    # optional argument to override default configuration
   <jar-file> <args>
   ```

## WebUI
Spark provides WebUI which is available via port `18080`.
To access it in your browser on `localhost:18080`, use `ssh` and call the following command:
```bash
ssh -L 18080:0.0.0.0:18080  ubuntu@<master-public-ip> -i ~/.ssh/<private-key>
```

To ensure which ports are used, you can use the following command:
```bash
netstat -tunalp | grep LISTEN
```
