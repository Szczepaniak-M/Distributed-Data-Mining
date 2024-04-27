# Hadoop Distributed File Systems

## OpenStack
### How to set proper environmental variables?
1. Get `openrc.sh` file
2. Add execute privileges:
    ```bash
    chmod +x openrc.sh
    ```
3. Execute a script in the context of the console to set variables
    ```bash
    . openrc.sh
    ```

## Terraform
### How to use?
1. [Install terraform on local machine](https://developer.hashicorp.com/terraform/install)
2. Configure OpenStack Provider environmental variables in Terraform by executing steps from [#Openstack](#openstack)
3. Modify the value in file `terraform.tfvars` to define VM detail
    ```bash
    image_id         # image id can be found at cloud provider
    instance_amount  # amount of instance would be create
    volume_size      # The size of volume which would be attached to instance (in Gigabytes)
    flavor_name      # instance type
    ```
4. Deploy VMs by running the following commands in the terminal:
    ```bash
    terraform init    # use at first time
    terraform plan    # check what resources will be create or destroy
    terraform apply   # apply changes
    ```
5. To print output again execute
   ```bash
    terraform output
   ```
6. After finishing using cluster, destroy all resources
    ```bash
    terraform destroy
    ```

## Ansible
1. Ansible is used for all setups. To install Ansible, use `pip`:
   ```bash
   pip install ansible
   ``` 
2. Edit `inventory.ini` to set proper values of IP addresses.
3. Run the Ansible playbook using the following command:
   ```bash
   ansible-playbook -i inventory.ini hadoop_setup.yml --key-file "~/.ssh/<private-key>"
   ```
   This will execute the playbook on the VMs specified in the inventory file
   The playbook:
   - install Java 11
   - add proper environment variables to `.bashrc`
   - download and configure Hadoop 3.3.6
   - create swap file
   - generate and distribute SSH keys
   - start HDFS NameNode, YARN ResourceManager, and MapReduce History Server
4. After finishing Ansible execution, HDFS is available at address: `master:9000`

## WebUI
The configuration allows to access three WebUIs:
- HDFS - available at port `9870`
- YARN - available at port `8088`
- MapReduce History Server - available at port `19888`

To access them in your browser on `localhost:<port>`, use `ssh` and call the following command:
```bash
ssh -L 9870:localhost:9870 -L 8088:master:8088 -L 19888:0.0.0.0:19888 ubuntu@<master-public-ip> -i ~/.ssh/<private-key>
```

To ensure which ports are used, you can use the following command:
```bash
netstat -tunalp | grep LISTEN
```

## Running MapReduce Job
1. Build jar file using Maven
   ```bash
   mvn package
   ```
2. Copy files to Master Node using `scp`
   ```bash
   scp -i ~/.ssh/<private-key> ./HDFS/<project-name>/target/<project-name>-1.0.jar ubuntu@<master-public-ip>:~
   ```
3. Run MapReduce job using the following command
   ```bash
   hadoop jar <jar-file> de.tum.ddm.<main-class> <args>
   ```
4. Output files are in `/output` HDFS directory
