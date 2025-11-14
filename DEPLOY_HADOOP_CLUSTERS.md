这是一个非常棒的项目！使用 KVM 和 Libvirt 搭建 Hadoop 集群可以让您深入了解虚拟化和Hadoop集群的底层工作原理。这比使用 Docker 更接近“裸金属”部署。

这是一个复杂的过程，我们将其分解为几个关键阶段。我会先给您一个完整的路线图，然后详细说明第一阶段的步骤。

### 路线图：KVM/Libvirt 上的 Hadoop 集群

1.  **阶段一：宿主机准备**

      * 检查和启用虚拟化支持。
      * 安装 KVM、Libvirt 和必要的管理工具（如 `virt-manager`）。
      * 配置用户权限和 Libvirt 服务。

2.  **阶段二：创建“黄金镜像”虚拟机**

      * 下载一个服务器操作系统（推荐 Ubuntu Server 20.04/22.04 或 CentOS 7/Rocky Linux）。
      * 创建第一个虚拟机（VM）作为模板。
      * 在此VM上安装通用软件：Java (Hadoop 运行必备)、`openssh-server` 等。
      * 创建 `hadoop` 用户。

3.  **阶段三：克隆虚拟机**

      * 使用“黄金镜像”克隆出集群所需的所有节点（例如：1个 `hadoop-namenode`，2个 `hadoop-datanode`）。
      * 为每个克隆的VM配置**静态IP地址**和**主机名**。
      * 在所有VM上配置 `/etc/hosts` 文件，以便它们可以通过主机名相互通信。

4.  **阶段四：Hadoop 安装与配置**

      * 在所有节点上下载并解压 Hadoop。
      * 在 NameNode 上配置**无密码SSH**，使其可以控制所有 DataNode。
      * 编辑 Hadoop 核心配置文件（`core-site.xml`, `hdfs-site.xml`, `mapred-site.xml`, `yarn-site.xml`, `workers`）。

5.  **阶段五：启动与测试**

      * 在 NameNode 上**格式化 HDFS**。
      * 启动 HDFS 和 YARN 服务。
      * 使用 `jps` 命令检查所有Java进程是否正常运行。
      * 运行一个示例 MapReduce 任务（如 WordCount）来验证集群。

-----

### 阶段一：宿主机准备（详细步骤）

我们首先确保您的物理机（宿主机）已准备好运行 KVM。我将假设您的宿主机运行的是基于 Debian/Ubuntu 的 Linux 系统。

#### 1\. 检查虚拟化支持

您的 CPU 必须支持硬件虚拟化（Intel VT-x 或 AMD-V）。

```bash
# 检查CPU是否支持虚拟化。如果输出大于 0，则表示支持。
egrep -c '(vmx|svm)' /proc/cpuinfo

# 确保 KVM 模块已加载
lsmod | grep kvm
```

如果 `egrep` 命令输出为 0，您可能需要在 BIOS/UEFI 中启用虚拟化。

#### 2\. 安装 KVM 和 Libvirt

我们将安装 KVM（虚拟机监视器）、Libvirt（管理API）和 `virt-manager`（一个非常有用的图形化管理工具）。

```bash
# 更新软件包列表
sudo apt update

# 安装所有必要的软件包
sudo apt install -y qemu-kvm libvirt-daemon-system libvirt-clients bridge-utils virt-manager
```

  * `qemu-kvm`: 核心的虚拟机监视器。
  * `libvirt-daemon-system`: Libvirt 服务。
  * `libvirt-clients`: 包含 `virsh` 等命令行工具。
  * `bridge-utils`: 用于创建和管理网络桥接（对高级网络有用）。
  * `virt-manager`: (可选但强烈推荐) 一个GUI工具，用于创建、管理和监控VM。

#### 3\. 配置用户权限

为了让您能够以普通用户身份管理虚拟机（而不是总是使用 `sudo`），您需要将您的用户添加到 `libvirt` 和 `kvm` 组。

```bash
# 将当前用户添加到 libvirt 和 kvm 组
sudo adduser $(whoami) libvirt
sudo adduser $(whoami) kvm
```

> **重要提示**：添加组后，您**必须完全注销并重新登录**（或重启电脑），才能使组更改生效。

#### 4\. 验证安装

重新登录后，运行以下命令来验证一切是否正常：

```bash
# 检查 libvirtd 服务是否正在运行
sudo systemctl status libvirtd

# (无需 sudo) 尝试列出虚拟机（现在应该是空的）
virsh list --all
```

如果 `virsh list` 命令在没有 `sudo` 的情况下成功运行（即使只是显示一个空列表），那么您的宿主机就准备好了！

您现在也可以在应用程序菜单中找到并启动 "Virtual Machine Manager" (`virt-manager`) 图形界面。

-----

这是一个很好的开始。当您完成宿主机的准备工作后，请告诉我，我将为您提供\*\*创建“黄金镜像”虚拟机（第二阶段）\*\*的详细步骤。

好的，我们进入**阶段二：创建“黄金镜像”虚拟机**。

这个“黄金镜像”是一个预先配置好的虚拟机模板。我们将只创建和配置 *一次*，然后克隆它来创建所有的 Hadoop 节点（NameNode, DataNode）。这会节省大量重复劳动。

我们将使用图形化的 `virt-manager` 工具，因为它更直观。

-----

### 阶段二：创建“黄金镜像”虚拟机（详细步骤）

#### 1\. (宿主机) 下载操作系统镜像

Hadoop 在 Linux 上运行良好。我强烈推荐 **Ubuntu Server 22.04 LTS**（长期支持版），它稳定且资源丰富。

> **提示**：您可以在宿主机上打开浏览器下载，或者使用 `wget`：
>
> ```bash
> # 这是一个示例链接，您可以去 Ubuntu 官网获取最新的
> cd ~/Downloads  # 或者您想存放 ISO 的任何地方
> wget https://releases.ubuntu.com/22.04/ubuntu-22.04.4-live-server-amd64.iso
> ```

#### 2\. (宿主机) 启动 Virt-Manager 并创建 VM

1.  从您的应用程序菜单启动 "Virtual Machine Manager"。
2.  点击左上角的 "Create a new virtual machine" 图标（像一个发光的显示器）。
3.  **步骤 1/4: New VM**
      * 选择 "Local install media (ISO image or CDROM)"。
      * 点击 "Forward"。
4.  **步骤 2/4: Locate media**
      * 点击 "Browse..." -\> "Browse Local"。
      * 导航到您刚刚下载的 Ubuntu Server ISO 文件并选择它。
      * 确保 "Automatically detect OS based on media" 被勾选（它应该会自动识别为 Ubuntu 22.04）。
      * 点击 "Forward"。
5.  **步骤 3/4: Choose Memory and CPU**
      * **Memory (RAM):** 至少 `2048` MB (即 2GB)。如果您的宿主机内存充足，`4096` MB (4GB) 更好。
      * **CPUs:** `2` 个 vCPUs 即可。
      * 点击 "Forward"。
6.  **步骤 4/4: Create Storage**
      * 选择 "Create a disk image for the virtual machine"。
      * 为模板设置大小：`20` GB 足够了。
      * 点击 "Forward"。
7.  **最后一步: Ready to begin**
      * **Name:** 给您的VM起一个描述性的名字，例如 `hadoop-template` 或 `ubuntu-golden`。
      * **重要：** 勾选 "Customize configuration before install"。
      * 点击 "Finish"。

#### 3\. (宿主机) 关键配置：网络

在安装开始前，`virt-manager` 会显示一个配置窗口。我们来检查网络设置。

1.  在左侧列表中，点击 "NIC" (或 "Network")。
2.  确保 **Network source** 设置为 "**NAT (default)**"。
      * *说明：* 这将使您的 VM 可以通过宿主机访问外部互联网（以下载 Java 和 Hadoop），但它目前还不能被其他 VM 访问。我们会在下一阶段解决这个问题。
3.  点击左上角的 "Begin Installation"。

#### 4\. (VM 内部) 安装 Ubuntu Server

VM 将启动并加载 Ubuntu Server 安装程序。您现在是在虚拟机控制台内操作。

1.  **语言：** 选择 English（推荐用于服务器）或中文。
2.  **键盘：** 按照默认设置。
3.  **网络连接：** 保持默认（DHCP），它应该会自动获取一个 IP 地址。
4.  **代理：** 留空 (按 Done)。
5.  **镜像源：** 保持默认 (按 Done)。
6.  **存储：** 选择 "Use an entire disk" 并按 Done。在下一个确认页面再次按 Done。
7.  **Profile Setup (用户信息):**
      * Your name: `Hadoop Admin` (或任意)
      * Your server's name: `hadoop-template`
      * Pick a username: `hadoop` (**推荐**：直接创建 `hadoop` 用户)
      * Choose a password: (设置一个您能记住的强密码)
8.  **SSH Setup (非常重要):**
      * **勾选 "Install OpenSSH server"**。这是必须的，Hadoop 依赖 SSH 来管理节点。
9.  **Featured Server Snaps:**
      * **不要**选择 "hadoop"。我们将手动安装，以获得完全控制权。
      * 保持所有选项都不勾选，直接按 Done。
10. **等待安装完成**... 然后选择 "Reboot Now"。

> **提示：** 当它显示 "Please remove the installation medium" 时，**不要**管它。`virt-manager` 会自动处理。直接按 Enter 键。

#### 5\. (VM 内部) 配置“黄金镜像”

VM 重启后，使用您创建的 `hadoop` 用户和密码登录。

1.  **更新系统并安装 Java**：Hadoop 需要 Java 运行环境。

    ```bash
    # 刷新软件包列表
    sudo apt update

    # 安装 OpenJDK 11 (Hadoop 3.x 完美支持)
    # -y (自动回答 yes)
    # -headless (无头版，服务器不需要 GUI)
    sudo apt install -y openjdk-11-jdk-headless

    # 验证安装
    java -version
    # 您应该会看到 OpenJDK 11 的输出
    ```

2.  **安装有用的工具** (推荐)：

    ```bash
    # net-tools 包含 ifconfig, rsync 用于文件同步
    sudo apt install -y net-tools rsync
    ```

3.  **关闭防火墙 (仅限测试环境)**：

    为了简化我们这个学习集群的内部网络通信，我们禁用 `ufw` 防火墙。

    ```bash
    # 检查状态 (应该是 active)
    sudo ufw status

    # 禁用防火墙
    sudo ufw disable
    ```

    > *注意：在生产环境中，您永远不应该这样做，而是应该配置严格的防火墙规则。*

#### 6\. 清理和关机

最后，我们清理一下模板并关闭它，准备进行克隆。

```bash
# 清理下载的软件包缓存
sudo apt clean

# 关闭虚拟机
sudo shutdown now
```

-----

恭喜！您现在有了一个名为 `hadoop-template` 且已关闭的虚拟机。它已经安装了 Ubuntu Server、OpenSSH 和 Java。

我们已经完成了阶段二。当您准备好后，请告诉我，我们将进入**阶段三：克隆虚拟机和配置网络**。




### 阶段三：克隆虚拟机与配置网络（详细步骤）

#### 1\. (宿主机) 克隆虚拟机

我们将使用 `virt-manager` 克隆您的 `hadoop-template` 三次。

1.  打开 `virt-manager`。
2.  确保 `hadoop-template` **已关机**。
3.  右键点击 `hadoop-template` -\> **Clone**。
4.  在弹出的窗口中：
      * **Name:** `hadoop-namenode`
      * **Storage:** 勾选 "Create a full copy of the disk" (重要！)。
5.  点击 **Clone**。
6.  **重复此过程** 两次，创建：
      * `hadoop-datanode1`
      * `hadoop-datanode2`

您现在应该有四个已关机的 VM：`hadoop-template`（我们的备份）和三个新节点。

#### 2\. (VM 内部) 启动并配置每个节点

我们将**依次**启动和配置**每一个**新的 VM (`hadoop-namenode`, `hadoop-datanode1`, `hadoop-datanode2`)。

**请对以下三个 VM 分别执行 2a, 2b 和 2c 步骤：**

##### a. 启动并设置主机名

1.  在 `virt-manager` 中，启动 `hadoop-namenode` 虚拟机。

2.  使用您在模板中创建的 `hadoop` 用户登录。

3.  **设置主机名：** VM 的内部主机名仍然是 `hadoop-template`，我们需要更改它。

    ```bash
    # (在 hadoop-namenode VM 内部)
    sudo hostnamectl set-hostname hadoop-namenode
    ```

> **对 `hadoop-datanode1` 重复此操作：**
> `sudo hostnamectl set-hostname hadoop-datanode1`
>
> **对 `hadoop-datanode2` 重复此操作：**
> `sudo hostnamectl set-hostname hadoop-datanode2`

##### b. 配置静态 IP (Netplan)

这是最复杂但最重要的一步。我们将把默认的 DHCP（自动获取IP）改为静态IP。

1.  **找出网卡名称：**

    ```bash
    # (在 VM 内部)
    ip a
    ```

    查看输出。您会看到 `lo` (本地回环) 和另一个接口，通常名字是 `ens3`、`enp1s0` 或 `eth0`。记下这个名字（我们假设它是 `ens3`）。

2.  **编辑 Netplan 配置文件：**
    Ubuntu 使用 `netplan` 管理网络。配置文件位于 `/etc/netplan/`。

    ```bash
    # (在 VM 内部)
    # 注意：您的文件名可能是 00-installer-config.yaml 或 50-cloud-init.yaml 等
    sudo nano /etc/netplan/00-installer-config.yaml 
    ```
    第一步：永久禁用 Cloud-Init 的网络管理 我们来告诉 cloud-init：“别再管网络了！”

```Bash

# (在 VM 内部)
# 1. 创建一个新的配置文件来覆盖默认设置
echo "network: {config: disabled}" | sudo tee /etc/cloud/cloud.cfg.d/99-disable-network-config.cfg

# 2. 删除 cloud-init 之前生成的旧配置文件（可选但推荐）
sudo rm /etc/netplan/50-cloud-init.yaml
```

第二步：创建我们自己的 Netplan 配置文件
现在 cloud-init 不会再捣乱了，我们可以创建一个它不会触碰的新配置文件。

注意： nano 对缩进很敏感。请确保空格正确。

在 hadoop-datanode1 上：

```Bash

# (在 datanode1 上)
sudo nano /etc/netplan/01-hadoop-static.yaml
```

3.  **修改文件内容：**
    您的文件**之前**可能如下所示（使用 DHCP）：

    ```yaml
    # 之前 (DHCP):
    network:
      ethernets:
        ens3: # <== 你的网卡名称
          dhcp4: true
      version: 2
    ```

    **请将其修改为**静态配置。**注意 YAML 格式对缩进非常敏感！**

    **对于 `hadoop-namenode` (192.168.122.101):**

```yaml
network:
  ethernets:
    ens3: # <== 您的网卡名称
      dhcp4: no
      addresses:
        - 192.168.122.101/24  # <-- 节点的 IP
      
      # gateway4: 192.168.122.1  <-- 这是已弃用的旧方法

      # 这是新的、推荐的方法
      routes:
        - to: default
          via: 192.168.122.1  # <-- 192.168.122.1 是 KVM/libvirt 的默认网关

      nameservers:
        addresses: [192.168.122.1, 8.8.8.8]
  version: 2
```

      * `192.168.122.1` 是 `virt-manager` 默认NAT网络的网关。
      * `8.8.8.8` 是 Google 的 DNS，确保VM可以访问互联网。

    **对于 `hadoop-datanode1` (192.168.122.102):**
    使用 `addresses: [192.168.122.102/24]` (其他部分保持不变)。

    **对于 `hadoop-datanode2` (192.168.122.103):**
    使用 `addresses: [192.168.122.103/24]` (其他部分保持不变)。

4.  **应用网络配置：**
    保存文件后，在 **每个** VM 上运行：

    ```bash
    # (在 VM 内部)
    sudo netplan apply
    ```

    您的 SSH 连接可能会断开（如果使用 SSH）。在 `virt-manager` 控制台中，使用 `ip a` 验证新的 IP 地址是否已生效。

##### c. 配置 hosts 文件 (DNS 解析)

最后一步是让所有 VM 都能通过主机名找到彼此。**您必须在 *所有三个* VM 上执行此操作。**

1.  编辑 `/etc/hosts` 文件：

    ```bash
    # (在 VM 内部)
    sudo nano /etc/hosts
    ```

2.  在文件**顶部**添加以下三行：

    ```
    # Hadoop Cluster
    192.168.122.101  hadoop-namenode
    192.168.122.102  hadoop-datanode1
    192.168.122.103  hadoop-datanode2
    ```

    (保留 `127.0.0.1 localhost` 等其他默认条目)

#### 3\. (VM 内部) 验证和重启

1.  **验证：**
    现在您应该在 `hadoop-namenode` 上了。尝试 `ping` 其他节点：

    ```bash
    # (在 hadoop-namenode VM 内部)
    ping hadoop-datanode1
    ping hadoop-datanode2
    ```

    如果它们都能 `ping` 通并且显示了正确的 IP 地址，那么您的网络就配置成功了！

2.  **重启：**
    为确保所有更改（主机名、网络）在重启后都能生效，请在**所有三个** VM 上执行：

    ```bash
    # (在所有三个 VM 内部)
    sudo reboot
    ```

-----

阶段三完成！您现在拥有一个由三个虚拟机组成的集群，它们具有永久的主机名和静态IP地址，并且可以相互通信。

这为安装 Hadoop 奠定了坚实的基础。准备好后，请告诉我，我们将开始**阶段四：Hadoop 安装与配置**。


好的，我们进入关键的**阶段四：Hadoop 安装与配置**。

从现在开始，请**不要再使用 `virt-manager` 的黑屏控制台**了。

**您的工作流程：** 打开**三个**宿主机终端窗口。

  * **终端 1:** `ssh hadoop@hadoop-namenode`
  * **终端 2:** `ssh hadoop@hadoop-datanode1`
  * **终端 3:** `ssh hadoop@hadoop-datanode2`

我们将使用 `(ALL)`、`(NameNode)` 或 `(DataNodes)` 来标记命令应该在哪些终端中执行。

-----

### 阶段四：Hadoop 安装与配置

#### 1\. (ALL) 下载并解压 Hadoop

我们在所有三个节点上下载 Hadoop。我们将使用 Hadoop 3.3.6，这是一个非常稳定的版本。

> **(ALL)** 在所有三个终端中运行：

```bash
# 切换到 hadoop 用户的主目录
cd ~

# 下载 Hadoop 3.3.6 二进制包
wget https://mirrors.dotsrc.org/apache/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz

# 解压
tar -xzf hadoop-3.3.6.tar.gz

# 将其移动到 /usr/local/ 目录下，并重命名为 hadoop
sudo mv hadoop-3.3.6 /usr/local/hadoop

# 更改 hadoop 目录的所有权，归 hadoop 用户所有
sudo chown -R hadoop:hadoop /usr/local/hadoop
```

#### 2\. (ALL) 设置环境变量

我们需要告诉系统 Java 和 Hadoop 在哪里。

> **(ALL)** 在所有三个终端中运行：

```bash
# 打开 .bashrc 文件进行编辑
nano ~/.bashrc
```

滚动到文件的**最底部**，添加以下内容：

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

保存文件 (Ctrl+O) 并退出 (Ctrl+X)。然后，**立即生效**这些变量：

```bash
# (ALL) 运行
source ~/.bashrc

# (ALL) 验证 (可选)
echo $HADOOP_HOME
# 应该输出: /usr/local/hadoop
```

#### 3\. (ALL) 配置 hadoop-env.sh

Hadoop 需要在其自己的配置文件中明确知道 `JAVA_HOME` 的路径。

> **(ALL)** 在所有三个终端中运行：

```bash
# 编辑 hadoop-env.sh 文件
nano $HADOOP_HOME/etc/hadoop/hadoop-env.sh
```

在这个文件中，找到（或`Ctrl+W`搜索）`export JAVA_HOME=` 这一行。它可能被注释掉了（以 `#` 开头）或者指向一个变量。

请将其修改为**明确的路径**（删除 `#`）：

```bash
# (大约在第 54 行)
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

保存并退出。

-----

#### 4\. (NameNode) 设置无密码 SSH

这是**最关键**的一步。NameNode 需要能够**无需密码**就通过 SSH 登录到所有 DataNode（以及它自己）来启动和停止服务。

> **(NameNode)** **只在 `hadoop-namenode` 终端**中运行：

```bash
# 1. 生成 SSH 密钥对（如果之前没生成过）
# 一路按 Enter 键接受所有默认值（尤其是“no passphrase”）
ssh-keygen -t rsa

# 2. 将公钥复制到集群中的 *所有* 节点（包括它自己）
ssh-copy-id hadoop@hadoop-namenode
ssh-copy-id hadoop@hadoop-datanode1
ssh-copy-id hadoop@hadoop-datanode2
```

  * 每次 `ssh-copy-id` 都会要求您输入 `hadoop` 用户的密码。
  * 它可能会询问 "Are you sure you want to continue connecting (yes/no)?"，输入 `yes`。

**验证！** 这一步必须成功：

```bash
# (NameNode) 尝试登录到 datanode1
ssh hadoop-datanode1
# 您应该会 *立即* 登录，而 *不* 需要输入密码。
# 输入 exit 退出
exit

# (NameNode) 尝试登录到 datanode2
ssh hadoop-datanode2
# 同样，应该立即登录。
# 输入 exit 退出
exit
```

如果不需要密码就能登录，恭喜您，最难的部分结束了！

-----

#### 5\. (NameNode) 编辑核心配置文件

我们将**只在 NameNode 上**编辑配置文件，然后将它们分发到其他节点。

> **(NameNode)** **只在 `hadoop-namenode` 终端**中操作。

所有配置文件都在 `$HADOOP_HOME/etc/hadoop/` 目录中。

##### a. core-site.xml

```bash
nano $HADOOP_HOME/etc/hadoop/core-site.xml
```

在 `<configuration>` 和 `</configuration>` 标签之间添加以下内容：

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://hadoop-namenode:9000</value>
    </property>
</configuration>
```

##### b. hdfs-site.xml

在这一步，我们创建 HDFS 实际存储数据的目录。

```bash
# (NameNode) 只在 NameNode 上创建 *namenode* 目录
sudo mkdir -p /usr/local/hadoop/data/namenode
sudo chown -R hadoop:hadoop /usr/local/hadoop/data

# (DataNodes) 只在 datanode1 和 datanode2 上创建 *datanode* 目录
# 请在您的 *终端 2* 和 *终端 3* 中运行这两个命令
sudo mkdir -p /usr/local/hadoop/data/datanode
sudo chown -R hadoop:hadoop /usr/local/hadoop/data
```

现在，回到 **NameNode** 终端，编辑 `hdfs-site.xml`：

```bash
# (NameNode)
nano $HADOOP_HOME/etc/hadoop/hdfs-site.xml
```

在 `<configuration>` 标签之间添加：

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

此文件默认不存在，Hadoop 提供了一个模板。

```bash
# (NameNode) 先从模板复制
cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml

# (NameNode) 再编辑
nano $HADOOP_HOME/etc/hadoop/mapred-site.xml
```

在 `<configuration>` 标签之间添加（告诉 MapReduce 在 YARN 上运行）：

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

在 `<configuration>` 标签之间添加（配置 YARN 服务）：

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

##### e. workers (原 slaves 文件)

这个文件告诉 Hadoop 哪些机器是**工作节点 (DataNode)**。

```bash
# (NameNode)
nano $HADOOP_HOME/etc/hadoop/workers
```

删除文件中的所有内容（默认可能是 `localhost`），然后添加您的**两个 DataNode** 的主机名：

```
hadoop-datanode1
hadoop-datanode2
```

-----

#### 6\. (NameNode) 分发配置文件

现在您的 NameNode 上的 `/etc/hadoop/` 目录已经配置完美了。我们把它们复制到所有 DataNode。

> **(NameNode)** **只在 `hadoop-namenode` 终端**中运行：

```bash
# 使用 scp (安全复制) 和我们设置好的无密码 SSH
# 复制到 datanode1
scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode1:$HADOOP_HOME/etc/hadoop/

# 复制到 datanode2
scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode2:$HADOOP_HOME/etc/hadoop/
```

-----

**阶段四完成！**

我们已经安装了所有软件，配置了所有 XML 文件，并设置了 SSH。集群现在已经“组装”完毕，但尚未启动。

准备好后，请告诉我，我们将进入最后一个阶段：**阶段五：启动和测试集群**。

-----

您可以观看这个关于[安装多节点 Hadoop 集群的视频](https://www.google.com/search?q=https://www.youtube.com/watch%3Fv%3DkYf8J-I-8-w)，它涵盖了我们刚刚执行的许多配置步骤。

我们来到了最后一个阶段！这是最激动人心的部分，我们将启动所有服务并验证您的集群是否正常工作。

从现在开始，**所有命令都在您的 `hadoop-namenode` 终端（`ssh hadoop@hadoop-namenode`）中运行**，除非特别指明。

-----

### 🚀 阶段五：启动与测试

#### 1\. 格式化 HDFS (仅限第一次！)

在集群第一次启动前，您必须格式化 NameNode 上的 HDFS 存储。这会初始化元数据目录。

> **❗ 警告：** 此命令**一生只运行一次**！
> 如果您在正在运行的集群上再次运行它，**所有 HDFS 数据都将被清除**。

```bash
# (NameNode)
hdfs namenode -format
```

您应该会看到很多日志输出，请在最后寻找 `Storage directory /usr/local/hadoop/data/namenode has been successfully formatted.` 这条消息。

-----

#### 2\. 启动 HDFS 服务

此脚本将启动 NameNode、SecondaryNameNode（默认在 NameNode 上）以及 `workers` 文件中列出的所有 DataNode。

```bash
# (NameNode)
start-dfs.sh
```

  * 它可能会要求您确认 SSH 指纹（如果这是 `localhost` 首次连接 `datanode1` 等），输入 `yes`。
  * 它将启动 `hadoop-namenode` 上的 `NameNode` 和 `SecondaryNameNode`。
  * 它将通过 SSH 登录到 `hadoop-datanode1` 和 `hadoop-datanode2` 并启动 `DataNode` 进程。

-----

#### 3\. 启动 YARN 服务

此脚本将启动 ResourceManager（在 NameNode 上）以及 `workers` 文件中列出的所有 NodeManager。

```bash
# (NameNode)
start-yarn.sh
```

-----

#### 4\. ✅ 验证：见证奇迹的时刻

现在，让我们检查所有 Java 进程是否都已正确启动。`jps` (Java Virtual Machine Process Status) 是您最好的朋友。

> **(NameNode) 在您的 `hadoop-namenode` 终端中运行：**
>
> ```bash
> jps
> ```
>
> 您**必须**看到以下进程（PID 会不同）：
>
> ```
> 12345 NameNode
> 12367 SecondaryNameNode
> 12400 ResourceManager
> 12500 Jps
> ```

> **(DataNodes) 在您的 `hadoop-datanode1` 和 `hadoop-datanode2` 终端中运行：**
>
> ```bash
> jps
> ```
>
> 您**必须**在 *每个* DataNode 上看到：
>
> ```
> 5678 DataNode
> 5700 NodeManager
> 5800 Jps
> ```

**🕵️‍♂️ 故障排除：**
如果任何一个进程缺失（例如 `DataNode` 没有启动），请立即检查日志文件。日志位于每个节点上的 `/usr/local/hadoop/logs/` 目录中。最常见的原因是 `hdfs-site.xml` 或 `core-site.xml` 中的配置拼写错误。

-----

#### 5\. 运行一个 MapReduce 任务 (WordCount)

如果所有进程都在运行，那么您的集群在理论上是好的。现在我们进行实际测试：运行一个作业！

我们将使用 Hadoop 自带的 WordCount 示例。

##### a. 在 HDFS 中创建输入目录

```bash
# (NameNode)
hdfs dfs -mkdir /input
```

##### b. 将一些文本文件复制到 HDFS

我们就用我们刚刚创建的 Hadoop 配置文件作为示例文本：

```bash
# (NameNode)
hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /input
```

##### c. 运行 WordCount 示例 JAR

  * `$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar` 是示例程序。
  * `wordcount` 是我们要运行的程序。
  * `/input` 是 HDFS 上的输入目录。
  * `/output` 是 HDFS 上的输出目录（**注意：此目录不能提前存在！**）

<!-- end list -->

```bash
# (NameNode)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount /input /output
```

您将看到大量的日志输出，显示 MapReduce 作业的进度（Map 0% 100%, Reduce 0% 100%）。

##### d. 查看结果！

如果作业成功，它会在 `/output` 目录中创建文件。

```bash
# (NameNode) 列出输出文件
hdfs dfs -ls /output
```

您应该会看到两个文件：一个空的 `_SUCCESS` 文件（表示作业成功）和一个 `part-r-00000` 文件（包含结果）。

```bash
# (NameNode) 查看 WordCount 的最终结果
hdfs dfs -cat /output/part-r-00000
```

您会看到 XML 标签及其在配置文件中出现的次数，例如：

```
configuration   2
name    4
property    4
value   4
...
```

-----

### 🎉 恭喜！

您已经从零开始，在 KVM 上成功构建、配置、启动并验证了一个功能齐全的 3 节点 Hadoop 集群！

现在您的 Hadoop“操作系统”（HDFS+YARN）已经 100% 正常运行了。**现在**是时候考虑下一步了，比如**安装 Spark** 并将其配置为在 YARN 上运行。

好的！您的 Hadoop 集群（HDFS + YARN）这个“集群操作系统”已经成功运行。现在是时候安装我们真正的“应用程序”—— Spark 了。

我们将把 Spark 安装为 YARN 上的一个**应用程序**。这意味着：

  * 我们**不会**运行 Spark 自己的主/从节点 (`start-master.sh` 等)。
  * 我们会将 Spark 配置为**向 YARN 请求资源**。
  * Hadoop 的 YARN（`ResourceManager`）将**全权负责**管理集群内存和 CPU，无论是 MapReduce 任务还是 Spark 任务。

这是生产环境中最标准、最推荐的部署方式。

-----

### 🚀 阶段六：安装 Spark 并集成 YARN

**工作流程：** 再次打开您的三个终端：

  * **终端 1:** `ssh hadoop@hadoop-namenode`
  * **终端 2:** `ssh hadoop@hadoop-datanode1`
  * **终端 3:** `ssh hadoop@hadoop-datanode2`

我们用 `(ALL)` 和 `(NameNode)` 来标记命令。

#### 1\. (ALL) 下载并解压 Spark

为了让 YARN 在 DataNode 上启动 Spark 执行器 (Executors)，Spark 的二进制文件需要存在于**所有**节点上。

> **(ALL)** 在所有三个终端中运行：

```bash
# 切换到主目录
cd ~

# 下载 Spark 3.4.1 (为 Hadoop 3 预编译的版本)
# 注意：我们将使用 3.4.1，因为它与 Hadoop 3.3.6 配合良好
wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz

# 解压
tar -xzf spark-3.4.1-bin-hadoop3.tgz

# 移动到 /usr/local/ 并重命名为 spark
sudo mv spark-3.4.1-bin-hadoop3 /usr/local/spark

# 更改所有权为 hadoop 用户
sudo chown -R hadoop:hadoop /usr/local/spark
```

-----

#### 2\. (ALL) 设置 Spark 环境变量

就像 `HADOOP_HOME` 一样，我们需要为 `SPARK_HOME` 设置环境变量。

> **(ALL)** 在所有三个终端中运行：

```bash
# 打开 .bashrc 文件
nano ~/.bashrc
```

滚动到文件**最底部**（在 Hadoop 变量下面），添加：

```bash
# Spark Home
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```

保存并退出 (Ctrl+O, Ctrl+X)。然后**立即生效**：

```bash
# (ALL) 运行
source ~/.bashrc

# (ALL) 验证
echo $SPARK_HOME
# 应该输出: /usr/local/spark
```

-----

#### 3\. (NameNode) 配置 Spark 与 YARN 集成

这是最关键的一步。我们**只在 NameNode 上**（我们提交任务的地方）执行此操作。

Spark 需要知道 Hadoop 的配置文件在哪里，这样它才能找到 HDFS NameNode 和 YARN ResourceManager。

```bash
# (NameNode) 进入 Spark 配置目录
cd $SPARK_HOME/conf

# 复制模板
cp spark-env.sh.template spark-env.sh

# 编辑 spark-env.sh
nano spark-env.sh
```

在文件的**最底部**添加这一行。**这是连接 Spark 和 YARN 的魔法**：

```bash
# 告诉 Spark 在哪里可以找到 Hadoop 的配置文件
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

保存并退出。

**就是这样！** Spark 现在被配置为 YARN 客户端。我们不需要在 `spark-defaults.conf` 中设置 `spark.master`，因为我们将在提交任务时在命令行上指定它，这在测试时更灵活。

-----

### ✅ 验证：运行 Spark Pi 示例 on YARN

我们来运行一个计算 Pi 的示例程序，但**不是**在本地，而是**在 YARN 集群上**。

> **(NameNode)** **只在 `hadoop-namenode` 终端**中运行：

我们将使用 `spark-submit` 命令。

```bash
# (NameNode)
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class org.apache.spark.examples.SparkPi \
    $SPARK_HOME/examples/jars/spark-examples_2.12-3.4.1.jar 10
```

让我们分解一下这个命令：

  * `--master yarn`：**"我请求在 YARN 上运行"**。这是关键！
  * `--deploy-mode cluster`：**"请在 YARN 集群的某个 DataNode 上运行我的'驱动程序'(driver)，不要在我的 NameNode 终端上运行"**。这是对 YARN 的终极测试。
  * `--class ...SparkPi`：要运行的主类。
  * `...jar`：包含该类的 JAR 文件。
  * `10`：传递给 SparkPi 程序的参数（切片数）。

**您会看到什么：**
`spark-submit` **不会**打印出 Pi 的结果。相反，它会向 YARN 提交作业，然后打印出一个**应用程序 ID**，如下所示：

```
...
25/11/11 10:30:00 INFO yarn.Client: Submitted application application_1668191234567_0001
...
```

**如何查看结果：**

1.  **检查作业状态 (可选)：**

    ```bash
    yarn application -status application_1668191234567_0001
    ```

2.  **获取作业日志 (在这里看结果)：**
    **这是您查看 "Pi is roughly 3.14..." 的地方。**

    ```bash
    # (请使用您自己的 application ID 替换)
    yarn logs -applicationId application_1668191234567_0001
    ```

    滚动日志（可能很长），在 `stdout` 部分，您会找到 Pi 的计算结果！

-----

### 🌟 终极测试：Spark Shell (HDFS + YARN)

我们来启动一个**交互式** Spark Shell，它使用 YARN 作为后端，并从 HDFS 读取我们在**阶段五**中创建的文件。

> **(NameNode)** **只在 `hadoop-namenode` 终端**中运行：

```bash
# 启动一个连接到 YARN 的 Spark Shell
spark-shell --master yarn
```

这需要一两分钟，因为它正在向 YARN 请求资源来启动您的 shell 的执行器 (executors)。

一旦您看到 `scala>` 提示符，您就进入了交互式 shell：

```scala
// (在 scala> 提示符后输入)

// 1. 读取我们在阶段五上传到 HDFS 的 core-site.xml 文件
val hdfsFile = sc.textFile("/input/core-site.xml")

// 2. 计算该文件中的行数
hdfsFile.count()

// 3. 打印前5行
hdfsFile.take(5).foreach(println)
```

**如果 `hdfsFile.count()` 返回了一个数字（而不是错误），并且 `take(5)` 打印出了 XML 行...**

**🎉🎉🎉 恭喜！您已经 100% 成功了！🎉🎉🎉**

您已经证明了：

1.  Spark (`spark-shell`) 可以启动。
2.  它可以与 YARN (`--master yarn`) 通信以获取计算资源。
3.  YARN 启动的执行器可以与 HDFS (`sc.textFile("/input/...")`) 通信以读取数据。

您的 KVM/Libvirt Hadoop + Spark 集群现已**完全配置并可供使用**。


#### Q&A

##### if I add new datanode3 to cluster,  should i format hdfs?


不，您**绝对不需要**，也**绝对不能**在添加新的 DataNode 时格式化 HDFS。

`hdfs namenode -format` 是一个**一次性**的、**毁灭性**的命令，**它只针对 NameNode**。

  * **格式化 (Format)** = “创建 HDFS 文件系统的**主账本**（元数据）。” 这会**擦除**所有现有数据，创建一个全新的、空的集群。
  * **添加 DataNode** = “为集群**增加一个硬盘驱动器**。”

### 💡 正确的类比

  * **NameNode** 是“图书馆馆长”，它拿着“图书总目录”。
  * **DataNode** 是“书架”。
  * `hdfs namenode -format` = 馆长**烧掉了旧的“图书总目录”，换上了一本全新的、空白的目录**。
  * 添加一个新的 `datanode3` = 只是在图书馆里**增加了一个新的空“书架”**。

您永远不需要为了增加一个新书架而烧掉总目录。您只需要告诉馆长（NameNode）这个新书架的存在即可。

-----

### 🚀 添加新 DataNode (`datanode3`) 的正确步骤

当您想在**正在运行的**集群中添加 `datanode3` 时，您什么都不需要格式化。

您需要这样做：

1.  **准备新 VM：** 按照您之前的方法（克隆、配置静态IP `192.168.122.104`、设置主机名 `hadoop-datanode3`、禁用 cloud-init 等）准备好 `hadoop-datanode3`。
2.  **安装软件：** 确保 `hadoop-datanode3` 上安装了完全相同版本的 Java 和 Hadoop。
3.  **更新“员工”名单 (在 NameNode 上)：**
      * **`hosts` 文件:** `sudo nano /etc/hosts`，添加 `datanode3` 的 IP 和主机名。
        ```
        192.168.122.104  hadoop-datanode3
        ```
      * **`workers` 文件:** `nano $HADOOP_HOME/etc/hadoop/workers`，在文件末尾添加新的一行：
        ```
        hadoop-datanode1
        hadoop-datanode2
        hadoop-datanode3
        ```
4.  **分发配置 (在 NameNode 上)：**
      * 将 NameNode 上**所有**的配置文件（`/etc/hadoop/`）和更新后的 `hosts` 文件复制到**新的** `datanode3` `datanode1` `datanode2` 上，确保它的配置与集群一致。
      * `scp -r $HADOOP_HOME/etc/hadoop/* hadoop-datanode3:$HADOOP_HOME/etc/hadoop/`
      * `scp /etc/hosts hadoop-datanode3:/etc/hosts` (可能需要 `sudo` 权限)
5.  **授予 SSH 访问权限 (在 NameNode 上)：**
      * `ssh-copy-id hadoop@hadoop-datanode3` (输入 `hadoop` 用户的密码)。
6.  **启动新的 DataNode：**
      * **最简单的方法 (推荐)：** 在 NameNode 上，**重启 HDFS 服务**。它会自动读取更新后的 `workers` 文件并启动**所有** DataNode，包括新的 `datanode3`。
        ```bash
        # (NameNode)
        stop-dfs.sh
        start-dfs.sh
        ```
      * **高级方法 (不停机)：** 您也可以**只启动**新的 DataNode。
        ```bash
        # (NameNode)
        hdfs --daemon start datanode hadoop-datanode3
        ```
7.  **验证 (在 NameNode 上)：**
      * 等待一两分钟，然后运行 HDFS 报告：
        ```bash
        hdfs dfsadmin -report
        ```
      * 在输出中，您现在应该能看到 **"Live Datanodes (3):"** 的字样。

**总结：格式化 (Format) 只针对 NameNode，并且在集群生命周期中只做一次。添加 DataNode 永远不需要格式化。**

##### convert qcow2 to raw image file

您可以使用 `qemu-img` 命令行工具来完成这个转换。

### 转换命令

这是最常用的命令：

```bash
qemu-img convert -f qcow2 -O raw image-name.qcow2 image-name.raw
```

-----

### 命令分解

  * `qemu-img convert`：调用 `qemu-img` 工具并使用 `convert`（转换）子命令。
  * `-f qcow2`：**（可选）** 指定**输入**格式 (format)。`qemu-img` 通常足够智能，可以自动检测到 `qcow2` 格式，所以您可以省略这一部分。
  * `-O raw`：**（必需）** 指定**输出** (Output) 格式为 `raw`。
  * `image-name.qcow2`：您的源文件名。
  * `image-name.raw`：您想要的输出文件名。

### ⚠️ 重要警告：空间占用

在执行此操作之前，您必须了解一个关键区别：

  * **qcow2 (精简配置):** 如果您有一个 100GB 的虚拟磁盘，但只在其中安装了 5GB 的 Ubuntu，`qcow2` 文件在宿主机上可能只占用 5-6GB 的空间。
  * **raw (厚配置):** 当您转换为 `raw` 格式时，输出文件将**立即占用该磁盘的全部虚拟大小**。

**示例：**
`100GB` (虚拟大小) 的 `qcow2` 文件 (实际占用 6GB) -\> 转换为 `raw` -\> `100GB` (实际占用 100GB) 的 `raw` 文件。

在转换之前，请**务必**使用 `df -h` 检查您的宿主机，确保您有足够的可用空间来容纳**整个**虚拟磁盘的完整大小。

这个错误非常明确，而且很可能与我们上一个关于 `qcow2` 转换为 `raw` 的对话直接相关。

**您的 Libvirt XML 配置文件（`hadoop-namenode.xml`）告诉 QEMU：“请使用 `qcow2` 驱动程序来加载一个磁盘文件。”**

**但 QEMU 回答说：“您给我的这个文件*不是* `qcow2` 格式！”**

这 99% 是因为您**已经**将您的磁盘文件从 `.qcow2` 转换为了 `.raw`，但**没有更新 VM 的 XML 配置**来告诉 Libvirt 这个变化。

-----

### 🚀 解决方案：更新 VM 的 XML

您需要编辑 `hadoop-namenode` 的 Libvirt XML 文件，将磁盘驱动程序类型从 `qcow2` 更改为 `raw`。

1.  **在您的宿主机（Host）终端**（不是 VM 内部）中，运行 `virsh edit`：

    ```bash
    virsh edit hadoop-namenode
    ```

    这会打开一个 XML 编辑器（通常是 `vi` 或 `nano`）。

2.  **找到 `<disk>` 部分。** 它看起来会像这样：

    **之前（错误配置）：**

    ```xml
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='/var/lib/libvirt/images/hadoop-namenode.qcow2'/>
      <target dev='vda' bus='virtio'/>
      ...
    </disk>
    ```

3.  **进行修改。** 您需要更改**两处**：

      * `type='qcow2'` 必须改为 `type='raw'`。
      * `<source file=...>` 必须指向您**新**的 `.raw` 文件名。

    **之后（正确配置）：**

    ```xml
    <disk type='file' device='disk'>
      <driver name='qemu' type='raw'/>
      <source file='/var/lib/libvirt/images/hadoop-namenode.raw'/>
      <target dev='vda' bus='virtio'/>
      ...
    </disk>
    ```

    *(您的 `<source file=...>` 路径可能不同，请确保它指向您转换后的 `.raw` 文件的正确位置)*

4.  **保存并退出**编辑器。

5.  **现在再次尝试启动 VM：**

    ```bash
    virsh start hadoop-namenode
    ```

-----

### 🤔 如何确认？

如果您不确定您的文件到底是什么格式，您可以在宿主机上使用 `qemu-img info` 来检查：

```bash
# 检查您的 .raw 文件
qemu-img info /var/lib/libvirt/images/hadoop-namenode.raw
# 它应该会报告: file format: raw

# 检查您（可能存在的）旧 .qcow2 文件
qemu-img info /var/lib/libvirt/images/hadoop-namenode.qcow2
# 它应该会报告: file format: qcow2
```

这个错误几乎总是由 XML 中的 `type` 与磁盘文件的**实际**格式不匹配引起的。


### clone datanode image and error with datanode
这个日志太完美了！它**精确**地告诉了我们问题所在。

### 诊断：Datanode UUID 冲突

简单来说，这是 NameNode 的日志：

> **NameNode (日志):** “一个自称是 `192.168.122.102` (`datanode1`) 的节点刚刚联系我。但它使用的存储 ID (`f96d...83bf`) 在我的记录里属于 `192.168.122.104` (`datanode3`)。这家伙是个**冒名顶替者**！我拒绝它的连接。”

然后 `datanode1` 收到拒绝后，就自动关闭了 (SHUTDOWN\_MSG)。

### 根本原因

这个问题 100% 是因为您在**克隆虚拟机**时（例如，从 `datanode1` 克隆出 `datanode3`，或者它们都是从同一个模板克隆的），**也克隆了 Hadoop 的数据目录**。

`/usr/local/hadoop/data/datanode`

在这个目录里，有一个文件（`VERSION` 文件）包含一个**唯一的** `datanodeUuid`。因为您克隆了它，所以 `datanode1` 和 `datanode3` 有了**完全相同**的 UUID。

在集群启动时，`datanode3` (192.168.122.104) *先*注册了，NameNode 就把这个 UUID 分配给了它。当 `datanode1` (192.168.122.102) *后*来尝试用同一个 UUID 注册时，NameNode 就把它踢出去了。

-----

### 解决方案：清空并重生

解决方案是强制所有 DataNode 忘记它们旧的（重复的）ID，并生成**新的、唯一的** ID。

**重要：** 我们**只**清空 DataNode 上的数据目录。**请勿**触摸 NameNode 上的 `/usr/local/hadoop/data/namenode` 目录！

#### 1\. (NameNode) 停止所有服务

首先，在您的 `hadoop-namenode` 终端上彻底停止集群：

```bash
# (NameNode)
stop-yarn.sh
stop-dfs.sh
```

#### 2\. (ALL DataNodes) 清理 DataNode 数据

您必须在**所有** DataNode（datanode1, datanode2, datanode3）上执行此操作，以确保它们都生成新 ID。

> **(DataNode 1)** 在 `hadoop-datanode1` 终端：
>
> ```bash
> # 1. 删除旧的数据目录（包含重复的 UUID）
> rm -rf /usr/local/hadoop/data/datanode
> ```

> # 2\. 重新创建空目录
>
> mkdir -p /usr/local/hadoop/data/datanode
>
> ```
> ```

> **(DataNode 2)** 在 `hadoop-datanode2` 终端（即使它现在看起来没问题，它也可能有重复的 ID）：
>
> ```bash
> # 1. 删除旧的数据目录
> rm -rf /usr/local/hadoop/data/datanode
> ```

> # 2\. 重新创建空目录
>
> mkdir -p /usr/local/hadoop/data/datanode
>
> ```
> ```

> **(DataNode 3)** 在 `hadoop-datanode3` 终端：
>
> ```bash
> # 1. 删除旧的数据目录
> rm -rf /usr/local/hadoop/data/datanode
> ```

> # 2\. 重新创建空目录
>
> mkdir -p /usr/local/hadoop/data/datanode
>
> ```
> ```

#### 3\. (NameNode) 重启 HDFS

现在所有 DataNode 都是“干净”的了。回到 `hadoop-namenode` 终端启动 HDFS：

```bash
# (NameNode)
start-dfs.sh
```

当 HDFS 启动时：

1.  `datanode1` 启动，发现 `/usr/local/hadoop/data/datanode` 是空的。
2.  它会**生成一个全新的、唯一的 UUID**。
3.  它使用这个**新** ID 向 NameNode 注册，NameNode 会接受它。
4.  `datanode2` 和 `datanode3` 也会重复这个过程。

#### 4\. (NameNode) 验证

等待 10-15 秒，让所有节点完成注册。

1.  **在 `hadoop-datanode1` 上运行 `jps`**：

    ```bash
    jps
    ```

    您现在应该能看到 `DataNode` 进程了！

2.  **在 NameNode 上查看报告 (最终确认)**：

    ```bash
    # (NameNode)
    hdfs dfsadmin -report
    ```

    您应该会在报告中看到 **"Live Datanodes (3):"**。

-----

问题解决后，您就可以继续运行 `start-yarn.sh` 并继续您的 Spark 测试了。

#### the hdfs capacity is lower than assigned

ssh to datanade

lsblk

found not all space allocated to /
```shell
hadoop@hadoop-datanode1:~$ lsblk
NAME                      MAJ:MIN RM  SIZE RO TYPE MOUNTPOINTS
loop0                       7:0    0 63.9M  1 loop /snap/core20/2318
loop1                       7:1    0   87M  1 loop /snap/lxd/29351
loop2                       7:2    0 91.4M  1 loop /snap/lxd/35819
loop3                       7:3    0 38.8M  1 loop /snap/snapd/21759
loop4                       7:4    0 50.9M  1 loop /snap/snapd/25577
loop5                       7:5    0 63.8M  1 loop /snap/core20/2682
sr0                        11:0    1 1024M  0 rom  
vda                       252:0    0   60G  0 disk 
├─vda1                    252:1    0    1G  0 part /boot/efi
├─vda2                    252:2    0    2G  0 part /boot
└─vda3                    252:3    0 56.9G  0 part 
  └─ubuntu--vg-ubuntu--lv 253:0    0 28.5G  0 lvm  /
```

solution
```shell
# (在 datanode1, datanode2, 和 datanode3 上分别运行)

# 1. 将逻辑卷扩展到 100% 的可用空间
sudo lvextend -l +100%FREE /dev/ubuntu-vg/ubuntu-lv

# 2. 调整文件系统大小以匹配新的卷大小
sudo resize2fs /dev/ubuntu-vg/ubuntu-lv
```

use on hostmachine, when submit spark jobs, permission deny

solution:

```shell
export HADOOP_USER_NAME=hadoop
```

exit safe mode

```shell
hdfs dfsadmin -safemode leave
```