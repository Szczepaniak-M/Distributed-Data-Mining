# Dask

## Setup Dask using Ansible
Run Ansible script for HDFS, then run Ansible for Dask.
Ansible sets up all needed Python packages and file configuration.
Remember to edit `inventory.ini` to set proper values of IP addresses.

To run Ansible, run the following command from `ansible` directory
```bash
ansible-playbook -i inventory.ini dask_setup.yml --key-file "~/.ssh/<private-key>"
```
Then you can run the Dask programs from a master node inside the cluster.

## Install Dask Manually
First update apt, then install pip and Dask
```bash
sudo apt-get update
sudo apt install python3-pip
python3 -m pip install "dask[complete]"
```

### Start a Dask cluster via SSH
Install paramiko on the main node
```bash
pip install paramiko
```
Create a separate host file, listing only the IP addresses of the nodes:
```
<main-node IP Address>
<worker-node1 IP Address>
<worker-node2 IP Address>
<worker-node3 IP Address>
...
```

## Run Dask
Start the Dask SSH process on the Main node:
```bash
dask ssh --hostfile /<path-to-hostfile> --ssh-username ubuntu --ssh-private-key  ~/.ssh/<private-key>
```
While using Ansible, host file can be found at `/home/ubuntu/dask/daskhosts`

Dask provides WebUI which is available via port `8787`.
To access it in your browser on `localhost:8787`, use `ssh` and call the following command:
```bash
ssh -L 8787:localhost:8787  ubuntu@<master-public-ip> -i ~/.ssh/<private-key>
```
