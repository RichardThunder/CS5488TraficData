# Hadoop and Spark Cluster Installation Guide on KVM/Libvirt

This guide provides step-by-step instructions for deploying a Hadoop and Spark cluster using KVM/Libvirt. This approach offers deep insight into virtualization and cluster mechanics, simulating a near-bare-metal environment. The process is divided into several key stages.

### Roadmap

1.  **Phase 1: Host Machine Preparation**
    *   Check and enable virtualization support.
    *   Install KVM, Libvirt, and necessary management tools (e.g., `virt-manager`).
    *   Configure user permissions and the Libvirt service.

2.  **Phase 2: Create a "Golden Image" VM**
    *   Download a server OS (Ubuntu Server 22.04 recommended).
    *   Create the first Virtual Machine (VM) to serve as a template.
    *   Install common software on this VM: Java, `openssh-server`, etc.
    *   Create a `hadoop` user.

3.  **Phase 3: Clone Virtual Machines**
    *   Clone the "golden image" to create all required cluster nodes (e.g., 1 `hadoop-namenode`, 2 `hadoop-datanode`).
    *   Configure a static IP address and hostname for each cloned VM.
    *   Configure the `/etc/hosts` file on all VMs for hostname resolution.

4.  **Phase 4: Hadoop Installation & Configuration**
    *   Download and extract Hadoop on all nodes.
    *   Configure passwordless SSH from the NameNode to all other nodes.
    *   Edit the core Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`, `workers`).

5.  **Phase 5: Startup & Testing**
    *   Format HDFS on the NameNode.
    *   Start HDFS and YARN services.
    *   Verify that all processes are running correctly.
    *   Run a sample MapReduce job to validate the cluster.

6.  **Phase 6: Install Spark and Integrate with YARN**
    *   Download and extract Spark on all nodes.
    *   Configure Spark to run on YARN.
    *   Run a sample Spark job to validate the integration.

---

### Phase 1: Host Machine Preparation (Detailed Steps)

First, ensure your physical machine (the host) is ready to run KVM. These instructions assume the host is running a Debian/Ubuntu-based Linux distribution.

#### 1. Check for Virtualization Support

Your CPU must support hardware virtualization (Intel VT-x or AMD-V).

```bash
# Check if the CPU supports virtualization. Output > 0 means it's supported.
egrep -c '(vmx|svm)' /proc/cpuinfo

# Ensure the KVM module is loaded
lsmod | grep kvm
```

If the `egrep` command outputs 0, you may need to enable virtualization in your BIOS/UEFI settings.

#### 2. Install KVM and Libvirt

Install KVM (the hypervisor), Libvirt (the management API), and `virt-manager` (a useful graphical management tool).

```bash
# Update package lists
sudo apt update

# Install all necessary packages
sudo apt install -y qemu-kvm libvirt-daemon-system libvirt-clients bridge-utils virt-manager
```

*   `qemu-kvm`: The core hypervisor.
*   `libvirt-daemon-system`: The Libvirt service.
*   `libvirt-clients`: Includes command-line tools like `virsh`.
*   `bridge-utils`: For creating and managing network bridges (useful for advanced networking).
*   `virt-manager`: (Optional but highly recommended) A GUI tool for creating, managing, and monitoring VMs.

#### 3. Configure User Permissions

To manage VMs as a regular user (without `sudo`), add your user to the `libvirt` and `kvm` groups.

```bash
# Add the current user to the libvirt and kvm groups
sudo adduser $(whoami) libvirt
sudo adduser $(whoami) kvm
```

> **Important:** After being added to the groups, you **must log out and log back in** (or reboot) for the changes to take effect.

#### 4. Verify Installation

After logging back in, run the following commands to verify that everything is working:

```bash
# Check if the libvirtd service is running
sudo systemctl status libvirtd

# (Without sudo) Try to list virtual machines (should be empty for now)
virsh list --all
```

If the `virsh list` command runs successfully without `sudo` (even if it just shows an empty list), your host machine is ready! You can also now find and launch the "Virtual Machine Manager" (`virt-manager`) GUI from your applications menu.

---

### Phase 2: Create a "Golden Image" VM (Detailed Steps)

This "golden image" is a pre-configured VM template. We will create and configure it *once*, then clone it to create all our Hadoop nodes (NameNode, DataNodes). This saves a lot of repetitive work. We will use the graphical `virt-manager` tool as it is more intuitive.

#### 1. (Host) Download the OS Image

Hadoop runs well on Linux. **Ubuntu Server 22.04 LTS** is highly recommended for its stability and community support.

> **Tip:** You can download it via a browser on your host machine, or use `wget`:
>
> ```bash
> # This is an example link, get the latest from the Ubuntu website
> cd ~/Downloads  # Or wherever you want to store the ISO
> wget https://releases.ubuntu.com/22.04/ubuntu-22.04.4-live-server-amd64.iso
> ```

#### 2. (Host) Launch Virt-Manager and Create the VM

1.  Launch "Virtual Machine Manager" from your applications menu.
2.  Click the "Create a new virtual machine" icon (looks like a glowing monitor) in the top-left.
3.  **Step 1/4: New VM**
    *   Select "Local install media (ISO image or CDROM)".
    *   Click "Forward".
4.  **Step 2/4: Locate media**
    *   Click "Browse..." -> "Browse Local".
    *   Navigate to and select the Ubuntu Server ISO file you just downloaded.
    *   Ensure "Automatically detect OS based on media" is checked (it should identify Ubuntu 22.04).
    *   Click "Forward".
5.  **Step 3/4: Choose Memory and CPU**
    *   **Memory (RAM):** At least `2048` MB (2GB). `4096` MB (4GB) is better if your host has enough memory.
    *   **CPUs:** `2` vCPUs are sufficient.
    *   Click "Forward".
6.  **Step 4/4: Create Storage**
    *   Select "Create a disk image for the virtual machine".
    *   Set the size for the template: `20` GB is enough.
    *   Click "Forward".
7.  **Final Step: Ready to begin**
    *   **Name:** Give your VM a descriptive name, e.g., `hadoop-template` or `ubuntu-golden`.
    *   **Important:** Check "Customize configuration before install".
    *   Click "Finish".

#### 3. (Host) Key Configuration: Network

Before the installation begins, `virt-manager` will show a configuration window. Let's check the network settings.

1.  In the left-hand list, click "NIC" (or "Network").
2.  Ensure **Network source** is set to "**NAT (default)**".
    *   *Explanation:* This will allow your VM to access the external internet (for downloading Java and Hadoop) through the host, but it won't be accessible by other VMs yet. We will fix this in the next phase.
3.  Click "Begin Installation" in the top-left.

#### 4. (Inside VM) Install Ubuntu Server

The VM will boot and load the Ubuntu Server installer. You are now operating inside the VM console.

1.  **Language:** Choose English (recommended for servers).
2.  **Keyboard:** Use the default layout.
3.  **Network Connections:** Keep the default (DHCP); it should get an IP address automatically.
4.  **Proxy:** Leave blank (press Done).
5.  **Mirror:** Keep the default (press Done).
6.  **Storage:** Select "Use an entire disk" and press Done. Press Done again on the confirmation screen.
7.  **Profile Setup:**
    *   Your name: `Hadoop Admin` (or anything)
    *   Your server's name: `hadoop-template`
    *   Pick a username: `hadoop` (**Recommended**: create the `hadoop` user directly)
    *   Choose a password: (Set a strong, memorable password)
8.  **SSH Setup (Very Important):**
    *   **Check "Install OpenSSH server"**. This is mandatory, as Hadoop relies on SSH to manage nodes.
9.  **Featured Server Snaps:**
    *   **Do NOT** select "hadoop". We will install it manually for full control.
    *   Leave all options unchecked and press Done.
10. **Wait for the installation to complete**... then select "Reboot Now".

> **Tip:** When it says "Please remove the installation medium", **ignore it**. `virt-manager` handles this automatically. Just press Enter.

#### 5. (Inside VM) Configure the "Golden Image"

After the VM reboots, log in with the `hadoop` user and password you created.

1.  **Update the system and install Java**: Hadoop requires a Java Runtime Environment.

    ```bash
    # Refresh package lists
    sudo apt update

    # Install OpenJDK 11 (perfectly compatible with Hadoop 3.x)
    # -y (auto-answer yes)
    # -headless (server doesn't need a GUI)
    sudo apt install -y openjdk-11-jdk-headless

    # Verify installation
    java -version
    # You should see output for OpenJDK 11
    ```

2.  **Install useful tools** (recommended):

    ```bash
    # net-tools contains ifconfig, rsync is for file syncing
    sudo apt install -y net-tools rsync
    ```

3.  **Disable the firewall (for test environments only)**:

    To simplify internal network communication for our learning cluster, we disable `ufw`.

    ```bash
    # Check status (should be active)
    sudo ufw status

    # Disable the firewall
    sudo ufw disable
    ```

    > *Note: In a production environment, you should never do this. Instead, configure strict firewall rules.*

#### 6. Clean Up and Shut Down

Finally, we clean up the template and shut it down, ready for cloning.

```bash
# Clean the downloaded package cache
sudo apt clean

# Shut down the VM
sudo shutdown now
```

You now have a powered-off VM named `hadoop-template` with Ubuntu Server, OpenSSH, and Java installed.

---

### Phase 3: Clone VMs and Configure Network (Detailed Steps)

#### 1. (Host) Clone the Virtual Machines

We will use `virt-manager` to clone your `hadoop-template` three times.

1.  Open `virt-manager`.
2.  Ensure `hadoop-template` is **powered off**.
3.  Right-click `hadoop-template` -> **Clone**.
4.  In the pop-up window:
    *   **Name:** `hadoop-namenode`
    *   **Storage:** Check "Create a full copy of the disk" (Important!).
5.  Click **Clone**.
6.  **Repeat this process** twice more to create:
    *   `hadoop-datanode1`
    *   `hadoop-datanode2`

You should now have four powered-off VMs: `hadoop-template` (our backup) and the three new nodes.

#### 2. (Inside VM) Start and Configure Each Node

We will start and configure **each** new VM (`hadoop-namenode`, `hadoop-datanode1`, `hadoop-datanode2`) **one by one**.

**Please perform steps 2a, 2b, and 2c for each of the three new VMs:**

##### a. Start and Set Hostname

1.  In `virt-manager`, start the `hadoop-namenode` VM.
2.  Log in as the `hadoop` user you created in the template.
3.  **Set the hostname:** The VM's internal hostname is still `hadoop-template`; we need to change it.

    ```bash
    # (Inside the hadoop-namenode VM)
    sudo hostnamectl set-hostname hadoop-namenode
    ```

> **Repeat for `hadoop-datanode1`:**
> `sudo hostnamectl set-hostname hadoop-datanode1`
>
> **Repeat for `hadoop-datanode2`:**
> `sudo hostnamectl set-hostname hadoop-datanode2`

##### b. Configure Static IP (Netplan)

This is the most complex but most important step. We will change the default DHCP (automatic IP) to a static IP.

1.  **Find the network interface name:**

    ```bash
    # (Inside the VM)
    ip a
    ```

    Look at the output. You will see `lo` (loopback) and another interface, typically named `ens3`, `enp1s0`, or `eth0`. Note this name (we'll assume it's `ens3`).

2.  **Edit the Netplan configuration file:**
    Ubuntu uses `netplan` to manage networking. The configuration files are in `/etc/netplan/`.

    ```bash
    # (Inside the VM)
    # Note: Your filename might be 00-installer-config.yaml or 50-cloud-init.yaml, etc.
    sudo nano /etc/netplan/00-installer-config.yaml
    ```
    First step: Permanently disable network management by Cloud-Init. Let's tell cloud-init: "Stop managing the network!"

    ```bash
    # (Inside the VM)
    # 1. Create a new config file to override the default
    echo "network: {config: disabled}" | sudo tee /etc/cloud/cloud.cfg.d/99-disable-network-config.cfg

    # 2. Remove the old config file generated by cloud-init (optional but recommended)
    sudo rm /etc/netplan/50-cloud-init.yaml
    ```

    Second step: Create our own Netplan configuration file. Now that cloud-init won't interfere, we can create a new configuration file that it won't touch.

    Note: `nano` is sensitive to indentation. Please ensure the spacing is correct.

    On hadoop-datanode1:
    ```bash
    # (On datanode1)
    sudo nano /etc/netplan/01-hadoop-static.yaml
    ```

3.  **Modify the file content:**
    Your file might **previously** have looked like this (using DHCP):

    ```yaml
    # Before (DHCP):
    network:
      ethernets:
        ens3: # <== your interface name
          dhcp4: true
      version: 2
    ```

    **Change it** to a static configuration. **Be very careful with YAML's indentation!**

    **For `hadoop-namenode` (192.168.122.101):**

    ```yaml
    network:
      ethernets:
        ens3: # <== Your interface name
          dhcp4: no
          addresses:
            - 192.168.122.101/24  # <-- Node's IP
          
          # gateway4: 192.168.122.1  <-- This is the old, deprecated method

          # This is the new, recommended method
          routes:
            - to: default
              via: 192.168.122.1  # <-- 192.168.122.1 is the default gateway for KVM/libvirt

          nameservers:
            addresses: [192.168.122.1, 8.8.8.8]
      version: 2
    ```

    *   `192.168.122.1` is the gateway for the `virt-manager` default NAT network.
    *   `8.8.8.8` is Google's DNS, ensuring the VM can access the internet.

    **For `hadoop-datanode1` (192.168.122.102):**
    Use `addresses: [192.168.122.102/24]` (the rest remains the same).

    **For `hadoop-datanode2` (192.168.122.103):**
    Use `addresses: [192.168.122.103/24]` (the rest remains the same).

4.  **Apply the network configuration:**
    After saving the file, run on **each** VM:

    ```bash
    # (Inside the VM)
    sudo netplan apply
    ```

    Your SSH connection might drop. In the `virt-manager` console, verify the new IP address with `ip a`.

##### c. Configure the hosts file (DNS Resolution)

The final step is to enable all VMs to find each other by hostname. **You must do this on *all three* VMs.**

1.  Edit the `/etc/hosts` file:

    ```bash
    # (Inside the VM)
    sudo nano /etc/hosts
    ```

2.  Add the following three lines at the **top** of the file:

    ```
    # Hadoop Cluster
    192.168.122.101  hadoop-namenode
    192.168.122.102  hadoop-datanode1
    192.168.122.103  hadoop-datanode2
    ```

    (Keep other default entries like `127.0.0.1 localhost`)

#### 3. (Inside VM) Verify and Reboot

1.  **Verification:**
    On `hadoop-namenode`, try to `ping` the other nodes:

    ```bash
    # (Inside the hadoop-namenode VM)
    ping hadoop-datanode1
    ping hadoop-datanode2
    ```

    If they can be pinged and show the correct IP addresses, your network is configured successfully!

2.  **Reboot:**
    To ensure all changes (hostname, network) persist after a reboot, execute on **all three** VMs:

    ```bash
    # (Inside all three VMs)
    sudo reboot
    ```

---

Phase three is complete! You now have a three-VM cluster with permanent hostnames, static IPs, and mutual connectivity. This provides a solid foundation for installing Hadoop.

---

### Phase 4: Hadoop Installation & Configuration

From now on, **stop using the `virt-manager` console**.

**Your workflow:** Open **three** terminal windows on your host machine.

*   **Terminal 1:** `ssh hadoop@hadoop-namenode`
*   **Terminal 2:** `ssh hadoop@hadoop-datanode1`
*   **Terminal 3:** `ssh hadoop@hadoop-datanode2`

We will use `(ALL)`, `(NameNode)`, or `(DataNodes)` to indicate where commands should be run.

#### 1. (ALL) Download and Extract Hadoop

Download Hadoop on all three nodes. We will use Hadoop 3.3.6, a very stable version.

> **(ALL)** Run in all three terminals:

```bash
# Switch to the hadoop user's home directory
cd ~

# Download the Hadoop 3.3.6 binary package
wget https://mirrors.dotsrc.org/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

# Extract
tar -xzf hadoop-3.3.6.tar.gz

# Move it to /usr/local/ and rename it to hadoop
sudo mv hadoop-3.3.6 /usr/local/hadoop

# Change ownership of the hadoop directory to the hadoop user
sudo chown -R hadoop:hadoop /usr/local/hadoop
```

#### 2. (ALL) Set Environment Variables

We need to tell the system where Java and Hadoop are located.

> **(ALL)** Run in all three terminals:

```bash
# Open the .bashrc file for editing
nano ~/.bashrc
```

Scroll to the **very bottom** of the file and add the following:

```bash
# Java Home
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Hadoop Home
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Save the file (Ctrl+O) and exit (Ctrl+X). Then, apply the variables **immediately**:

```bash
# (ALL) Run
source ~/.bashrc

# (ALL) Verify (optional)
echo $HADOOP_HOME
# Should output: /usr/local/hadoop
```

#### 3. (ALL) Configure hadoop-env.sh

Hadoop needs to know the `JAVA_HOME` path explicitly in its own configuration file.

> **(ALL)** Run in all three terminals:

```bash
# Edit the hadoop-env.sh file
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

In this file, find the line `export JAVA_HOME=`. It might be commented out (starts with `#`) or point to a variable.

Modify it to the **explicit path** (and remove the `#`):

```bash
# (Around line 54)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

Save and exit.

---

#### 4. (NameNode) Set Up Passwordless SSH

This is the **most critical** step. The NameNode needs to be able to SSH into all DataNodes (and itself) **without a password** to start and stop services.

> **(NameNode)** Run **only in the `hadoop-namenode` terminal**:

```bash
# 1. Generate an SSH key pair (if you haven't already)
# Press Enter all the way through to accept defaults (especially "no passphrase")
ssh-keygen -t rsa

# 2. Copy the public key to *all* nodes in the cluster (including itself)
ssh-copy-id hadoop@hadoop-namenode
ssh-copy-id hadoop@hadoop-datanode1
ssh-copy-id hadoop@hadoop-datanode2
```

*   Each `ssh-copy-id` will ask for the `hadoop` user's password.
*   It may ask "Are you sure you want to continue connecting (yes/no)?", type `yes`.

**VERIFY!** This step must succeed:

```bash
# (NameNode) Try to log into datanode1
ssh hadoop-datanode1
# You should log in *immediately* without a password prompt.
# Type exit to return
exit

# (NameNode) Try to log into datanode2
ssh hadoop-datanode2
# Again, should be immediate.
# Type exit to return
exit
```

If you can log in without a password, congratulations, the hardest part is over!

---

#### 5. (NameNode) Edit Core Configuration Files

We will edit the configuration files **only on the NameNode**, then distribute them to the other nodes.

> **(NameNode)** Operate **only in the `hadoop-namenode` terminal**.

All configuration files are in the `$HADOOP_HOME/etc/hadoop/` directory.

##### a. core-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

Add the following between the `<configuration>` and `</configuration>` tags:

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-namenode:9000</value>
    </property>
</configuration>
```

##### b. hdfs-site.xml

In this step, we create the directories where HDFS will actually store its data.

```bash
# (NameNode) Create the *namenode* directory only on the NameNode
sudo mkdir -p /usr/local/hadoop/data/namenode
sudo chown -R hadoop:hadoop /usr/local/hadoop/data

# (DataNodes) Create the *datanode* directory only on datanode1 and datanode2
# Please run these two commands in your *Terminal 2* and *Terminal 3*
sudo mkdir -p /usr/local/hadoop/data/datanode
sudo chown -R hadoop:hadoop /usr/local/hadoop/data
```

Now, back in the **NameNode** terminal, edit `hdfs-site.xml`:

```bash
# (NameNode)
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

Add between the `<configuration>` tags:

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/usr/local/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/usr/local/hadoop/data/datanode</value>
    </property>
</configuration>
```

##### c. mapred-site.xml

This file doesn't exist by default; Hadoop provides a template.

```bash
# (NameNode) First, copy from the template
cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml

# (NameNode) Then, edit
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

Add between the `<configuration>` tags (to tell MapReduce to run on YARN):

```xml
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
</configuration>
```

##### d. yarn-site.xml

```bash
# (NameNode)
nano $HADOOP_HOME/etc/hadoop/yarn-site.xml
```

```bash
hdfs dfs -mkdir -p /yarn-logs
```

Add between the `<configuration>` tags (to configure YARN services):

```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>hadoop-namenode</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.log-aggregation-enable</name>
        <value>true</value>
    </property>
    <property>
        <name>yarn.nodemanager.remote-app-log-dir</name>
        <value>hdfs://hadoop-namenode:9000/yarn-logs</value>
    </property>
    <property>
        <name>yarn.log-aggregation.retain-seconds</name>
        <value>86400</value> 
    </property>
</configuration>
```

##### e. workers (formerly slaves)

This file tells Hadoop which machines are **worker nodes (DataNodes)**.

```bash
# (NameNode)
nano $HADOOP_HOME/etc/hadoop/workers
```

Delete all content in the file (default might be `localhost`) and add the hostnames of your **two DataNodes**:

```
hadoop-datanode1
hadoop-datanode2
```

---

#### 6. (NameNode) Distribute Configuration Files

Now that the `/etc/hadoop/` directory on your NameNode is perfectly configured, let's copy it to all DataNodes.

> **(NameNode)** Run **only in the `hadoop-namenode` terminal**:

```bash
# Use scp (secure copy) with our passwordless SSH setup
# Copy to datanode1
scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode1:$HADOOP_HOME/etc/hadoop/

# Copy to datanode2
scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode2:$HADOOP_HOME/etc/hadoop/
```

---

**Phase four is complete!** We have installed all software, configured all XML files, and set up SSH. The cluster is now "assembled" but not yet started.

---

### 🚀 Phase 5: Startup and Testing

This is the most exciting part. We will start all services and verify that your cluster is working.

From now on, **all commands are run in your `hadoop-namenode` terminal (`ssh hadoop@hadoop-namenode`)**, unless specified otherwise.

#### 1. Format HDFS (First Time Only!)

Before the cluster's first startup, you must format the HDFS storage on the NameNode. This initializes the metadata directory.

> **❗ Warning:** This command is **run only once in the lifetime of the cluster**!
> If you run it again on a running cluster, **all HDFS data will be erased**.

```bash
# (NameNode)
hdfs namenode -format
```

You should see a lot of log output. Look for the message `Storage directory /usr/local/hadoop/data/namenode has been successfully formatted.` at the end.

---

#### 2. Start HDFS Services

This script will start the NameNode, the SecondaryNameNode (on the NameNode by default), and all DataNodes listed in the `workers` file.

```bash
# (NameNode)
start-dfs.sh
```

*   It may ask you to confirm SSH fingerprints; type `yes`.
*   It will start the `NameNode` and `SecondaryNameNode` on `hadoop-namenode`.
*   It will SSH into `hadoop-datanode1` and `hadoop-datanode2` to start the `DataNode` process.

---

#### 3. Start YARN Services

This script will start the ResourceManager (on the NameNode) and all NodeManagers listed in the `workers` file.

```bash
# (NameNode)
start-yarn.sh
```

---

#### 4. ✅ Verification: The Moment of Truth

Now, let's check if all Java processes have started correctly. `jps` (Java Virtual Machine Process Status) is your best friend.

> **(NameNode)** In your `hadoop-namenode` terminal, run:
>
> ```bash
> jps
> ```
>
> You **must** see the following processes (PIDs will differ):
>
> ```
> 12345 NameNode
> 12367 SecondaryNameNode
> 12400 ResourceManager
> 12500 Jps
> ```

> **(DataNodes)** In your `hadoop-datanode1` and `hadoop-datanode2` terminals, run:
>
> ```bash
> jps
> ```
>
> You **must** see on *each* DataNode:
>
> ```
> 5678 DataNode
> 5700 NodeManager
> 5800 Jps
> ```

**🕵️‍♂️ Troubleshooting:**
If any process is missing (e.g., `DataNode` didn't start), check the log files immediately. Logs are in the `/usr/local/hadoop/logs/` directory on each node. The most common cause is a typo in `hdfs-site.xml` or `core-site.xml`.

---

#### 5. Run a MapReduce Job (WordCount)

If all processes are running, your cluster is theoretically sound. Now for a real test: running a job! We'll use the WordCount example included with Hadoop.

##### a. Create an input directory in HDFS

```bash
# (NameNode)
hdfs dfs -mkdir /input
```

##### b. Copy some text files into HDFS

Let's use our Hadoop configuration files as sample text:

```bash
# (NameNode)
hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /input
```

##### c. Run the WordCount example JAR

*   `$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar` is the example program.
*   `wordcount` is the program we want to run.
*   `/input` is the input directory on HDFS.
*   `/output` is the output directory on HDFS (**Note: this directory must not exist beforehand!**)

```bash
# (NameNode)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount /input /output
```

You will see a lot of log output showing the MapReduce job progress (Map 0% -> 100%, Reduce 0% -> 100%).

##### d. View the results!

If the job succeeded, it created files in the `/output` directory.

```bash
# (NameNode) List the output files
hdfs dfs -ls /output
```

You should see two files: an empty `_SUCCESS` file (indicating success) and a `part-r-00000` file (containing the results).

```bash
# (NameNode) View the final WordCount result
hdfs dfs -cat /output/part-r-00000
```

You will see the XML tags and their counts, e.g.:

```
configuration   2
name    4
property    4
value   4
...
```

---

### 🎉 Congratulations!

You have successfully built, configured, started, and validated a fully functional 3-node Hadoop cluster from scratch on KVM! Your Hadoop "operating system" (HDFS+YARN) is 100% operational.

---

### 🚀 Phase 6: Install Spark and Integrate with YARN

With your Hadoop cluster (HDFS + YARN) running, it's time to install Spark as an application on top of it. This is the standard and recommended deployment method for production environments.

**Workflow:** Open your three terminals again:

*   **Terminal 1:** `ssh hadoop@hadoop-namenode`
*   **Terminal 2:** `ssh hadoop@hadoop-datanode1`
*   **Terminal 3:** `ssh hadoop@hadoop-datanode2`

#### 1. (ALL) Download and Extract Spark

The Spark binaries need to exist on **all** nodes so YARN can launch Spark Executors on the DataNodes.

> **(ALL)** Run in all three terminals:

```bash
# Switch to home directory
cd ~

# Download Spark 3.4.1 (pre-built for Hadoop 3)
wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

# Extract
tar -xzf spark-3.4.1-bin-hadoop3.tgz

# Move to /usr/local/ and rename to spark
sudo mv spark-3.4.1-bin-hadoop3 /usr/local/spark

# Change ownership to the hadoop user
sudo chown -R hadoop:hadoop /usr/local/spark
```

---

#### 2. (ALL) Set Spark Environment Variables

Just like `HADOOP_HOME`, we need to set `SPARK_HOME`.

> **(ALL)** Run in all three terminals:

```bash
# Open .bashrc file
nano ~/.bashrc
```

Scroll to the **very bottom** (below the Hadoop variables) and add:

```bash
# Spark Home
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```

Save and exit (Ctrl+O, Ctrl+X). Then **apply immediately**:

```bash
# (ALL) Run
source ~/.bashrc

# (ALL) Verify
echo $SPARK_HOME
# Should output: /usr/local/spark
```

---

#### 3. (NameNode) Configure Spark to Integrate with YARN

This is the key step. We do this **only on the NameNode** (where we will submit jobs). Spark needs to know where to find Hadoop's configuration files to locate the HDFS NameNode and YARN ResourceManager.

```bash
# (NameNode) Go to Spark's configuration directory
cd $SPARK_HOME/conf

# Copy the template
cp spark-env.sh.template spark-env.sh

# Edit spark-env.sh
nano spark-env.sh
```

Add this line at the **very bottom** of the file. **This is the magic that connects Spark to YARN**:

```bash
# Tell Spark where to find Hadoop's configuration files
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

Save and exit. That's it! Spark is now configured as a YARN client.

---

### ✅ Verification: Run Spark Pi Example on YARN

Let's run a sample program to calculate Pi, but **on the YARN cluster**, not locally.

> **(NameNode)** Run **only in the `hadoop-namenode` terminal**:

```bash
# (NameNode)
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.12-3.4.1.jar 10
```

Let's break down the command:
*   `--master yarn`: **"I want to run on YARN."** This is the key!
*   `--deploy-mode cluster`: **"Please run my driver program on a DataNode in the cluster, not in my NameNode terminal."** This is the ultimate test for YARN.
*   `--class ...SparkPi`: The main class to run.
*   `...jar`: The JAR file containing the class.
*   `10`: An argument passed to the SparkPi program.

**What you will see:**
`spark-submit` will **not** print the result of Pi. Instead, it submits the job to YARN and prints an **application ID**, like this:

```
...
25/11/11 10:30:00 INFO yarn.Client: Submitted application application_1668191234567_0001
...
```

**How to see the result:**

1.  **Check job status (optional):**
    ```bash
    yarn application -status application_1668191234567_0001
    ```

2.  **Get job logs (this is where you see the result):**
    ```bash
    # (Replace with your own application ID)
    yarn logs -applicationId application_1668191234567_0001
    ```
    Scroll through the logs (they can be long), and in the `stdout` section, you will find the calculated value of Pi!

---

### 🌟 Ultimate Test: Spark Shell (HDFS + YARN)

Let's start an **interactive** Spark Shell that uses YARN as its backend and reads the files we created in HDFS during **Phase 5**.

> **(NameNode)** Run **only in the `hadoop-namenode` terminal**:

```bash
# Start a Spark Shell connected to YARN
spark-shell --master yarn
```

This may take a minute or two as it requests resources from YARN to start the shell's executors.

Once you see the `scala>` prompt, you are in the interactive shell:

```scala
// (Enter this at the scala> prompt)

// 1. Read the core-site.xml file we uploaded to HDFS in Phase 5
val hdfsFile = sc.textFile("/input/core-site.xml")

// 2. Count the number of lines in the file
hdfsFile.count()

// 3. Print the first 5 lines
hdfsFile.take(5).foreach(println)
```

If `hdfsFile.count()` returns a number (not an error) and `take(5)` prints XML lines...

**🎉🎉🎉 Congratulations! You have 100% succeeded! 🎉🎉🎉**

You have proven that:
1.  Spark (`spark-shell`) can start.
2.  It can communicate with YARN (`--master yarn`) to get computing resources.
3.  The executors launched by YARN can communicate with HDFS (`sc.textFile("/input/...")`) to read data.

Your KVM/Libvirt Hadoop + Spark cluster is now **fully configured and ready for use**.

---

### Q&A and Troubleshooting

##### If I add a new datanode3 to the cluster, should I format HDFS?

No, you **absolutely do not** and **must not** format HDFS when adding a new DataNode.

`hdfs namenode -format` is a **one-time, destructive** command that **only targets the NameNode**.

*   **Formatting** = "Create the master ledger (metadata) for the HDFS filesystem." This **erases** all existing data and creates a brand new, empty cluster.
*   **Adding a DataNode** = "Add a new hard drive to the cluster."

**Correct Analogy:**
*   **NameNode** is the "head librarian" with the "master card catalog."
*   **DataNode** is a "bookshelf."
*   `hdfs namenode -format` = The librarian **burns the old card catalog and replaces it with a new, blank one.**
*   Adding a new `datanode3` = Simply **adding a new, empty bookshelf** to the library.

You never need to burn the card catalog just to add a new bookshelf.

---

##### Correct Steps to Add a New DataNode (`datanode3`)

1.  **Prepare the new VM:** Prepare `hadoop-datanode3` just as you did before (clone, configure static IP `192.168.122.104`, set hostname `hadoop-datanode3`, etc.).
2.  **Install Software:** Ensure the exact same versions of Java and Hadoop are installed on `hadoop-datanode3`.
3.  **Update "Employee" Lists (on NameNode):**
    *   **`hosts` file:** `sudo nano /etc/hosts`, add the IP and hostname for `datanode3`.
        ```
        192.168.122.104  hadoop-datanode3
        ```
    *   **`workers` file:** `nano $HADOOP_HOME/etc/hadoop/workers`, add a new line at the end:
        ```
        hadoop-datanode1
        hadoop-datanode2
        hadoop-datanode3
        ```
4.  **Distribute Configurations (on NameNode):**
    *   Copy **all** configuration files (`/etc/hadoop/`) and the updated `hosts` file from the NameNode to the **new** `datanode3`, `datanode1`, and `datanode2` to ensure consistency.
    *   `scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode3:$HADOOP_HOME/etc/hadoop/`
    *   `scp /etc/hosts hadoop-datanode3:/etc/hosts` (may require `sudo` permissions)
5.  **Grant SSH Access (on NameNode):**
    *   `ssh-copy-id hadoop@hadoop-datanode3` (enter the `hadoop` user's password).
6.  **Start the New DataNode:**
    *   **Easiest Method (Recommended):** On the NameNode, **restart the HDFS services**. It will automatically read the updated `workers` file and start **all** DataNodes, including the new `datanode3`.
        ```bash
        # (NameNode)
        stop-dfs.sh
        start-dfs.sh
        ```
7.  **Verify (on NameNode):**
    *   Wait a minute, then run the HDFS report:
        ```bash
        hdfs dfsadmin -report
        ```
    *   In the output, you should now see **"Live Datanodes (3):"**.

**Summary: Formatting is for the NameNode only and is done once in the cluster's lifetime. Adding DataNodes never requires formatting.**

---

##### How to convert a qcow2 to a raw image file

You can use the `qemu-img` command-line tool for this conversion.

**Command:**
```bash
qemu-img convert -f qcow2 -O raw image-name.qcow2 image-name.raw
```

**Command Breakdown:**
*   `qemu-img convert`: The command to convert an image.
*   `-f qcow2`: **(Optional)** Specifies the input format. `qemu-img` is usually smart enough to detect this.
*   `-O raw`: **(Required)** Specifies the output format as `raw`.
*   `image-name.qcow2`: Your source file.
*   `image-name.raw`: Your desired output file.

**⚠️ Important Warning: Disk Space**
*   **qcow2 (Thin Provisioning):** A 100GB virtual disk with only 5GB of data might only take up 5-6GB of space on the host.
*   **raw (Thick Provisioning):** When you convert to `raw`, the output file will **immediately occupy the full virtual size** of the disk.

**Example:**
A `100GB` (virtual size) `qcow2` file (using 6GB) -> converted to `raw` -> a `100GB` (using 100GB) `raw` file.

**Always** check your available host disk space with `df -h` before converting.

---

##### Error: `qemu-kvm: -drive file=...: driver 'qcow2' can't be used with a RAW file`

This error means your Libvirt XML configuration is telling QEMU to use the `qcow2` driver, but the disk file itself is in `raw` format. This mismatch happens if you converted the disk image but didn't update the VM's configuration.

**Solution: Update the VM's XML**

1.  **On your host machine**, run `virsh edit`:
    ```bash
    virsh edit hadoop-namenode
    ```
2.  **Find the `<disk>` section.** It will look something like this:
    **Before (Incorrect):**
    ```xml
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='/var/lib/libvirt/images/hadoop-namenode.qcow2'/>
      ...
    </disk>
    ```
3.  **Make two changes:**
    *   Change `type='qcow2'` to `type='raw'`.
    *   Update the `<source file=...>` to point to your new `.raw` filename.
    **After (Correct):**
    ```xml
    <disk type='file' device='disk'>
      <driver name='qemu' type='raw'/>
      <source file='/var/lib/libvirt/images/hadoop-namenode.raw'/>
      ...
    </disk>
    ```
4.  **Save and exit** the editor.
5.  **Try starting the VM again:**
    ```bash
    virsh start hadoop-namenode
    ```

---

##### Error: Datanode shuts down, log shows "Incompatible Jackson version" or "Datanode UUID mismatch"

**Diagnosis: Datanode UUID Conflict**
The NameNode log shows it's rejecting a DataNode because its storage ID is already registered to another DataNode with a different IP.

**Root Cause:**
This happens when you clone a VM (e.g., cloning `datanode1` to create `datanode3`) and also clone the Hadoop data directory (`/usr/local/hadoop/data/datanode`). This directory contains a `VERSION` file with a unique `datanodeUuid`. Cloning results in multiple DataNodes having the same UUID. The NameNode will only allow the first one that registers to connect.

**Solution: Clear DataNode Storage and Regenerate IDs**
**Important:** This only applies to the **DataNode** data directories. **Do NOT** touch the NameNode's data directory (`/usr/local/hadoop/data/namenode`).

1.  **(NameNode)** Stop all services:
    ```bash
    # (NameNode)
    stop-yarn.sh
    stop-dfs.sh
    ```
2.  **(ALL DataNodes)** On **every** DataNode (datanode1, datanode2, datanode3), clean the data directory:
    ```bash
    # (On each DataNode)
    # 1. Remove the old data directory (with the duplicate UUID)
    rm -rf /usr/local/hadoop/data/datanode
    # 2. Recreate the empty directory
    mkdir -p /usr/local/hadoop/data/datanode
    ```
3.  **(NameNode)** Restart HDFS:
    ```bash
    # (NameNode)
    start-dfs.sh
    ```
    When the DataNodes start, they will find their data directory is empty, generate a new unique UUID, and register successfully with the NameNode.
4.  **(NameNode)** Verify:
    Wait a few moments, then check the report:
    ```bash
    # (NameNode)
    hdfs dfsadmin -report
    ```
    You should see **"Live Datanodes (3):"** in the report. You can also run `jps` on each DataNode to confirm the `DataNode` process is running.

---

##### HDFS capacity is lower than assigned disk space

If `lsblk` on a DataNode shows unallocated space in the logical volume, you need to extend the filesystem.

**Solution:**
```shell
# (Run on each affected DataNode)

# 1. Extend the logical volume to use 100% of the free space
sudo lvextend -l +100%FREE /dev/ubuntu-vg/ubuntu-lv

# 2. Resize the filesystem to match the new volume size
sudo resize2fs /dev/ubuntu-vg/ubuntu-lv
```

---

##### Permission denied when submitting Spark jobs from host machine

**Solution:**
Set the `HADOOP_USER_NAME` environment variable to match the user running the Hadoop cluster.
```shell
export HADOOP_USER_NAME=hadoop
```

---

##### How to exit safe mode

```shell
hdfs dfsadmin -safemode leave
```