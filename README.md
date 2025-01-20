[![Moleculer](https://badgen.net/badge/Powered%20by/Moleculer/0e83cd)](https://moleculer.services)

# block-storage

## Overview

block-storage is a microservice-based storage management system built using the Moleculer framework. It provides functionalities to manage NFS servers, folders, disks, and block storage devices. The system is designed to be highly scalable and flexible, allowing for easy integration with various storage backends and orchestration platforms like Kubernetes. Currently only Kubernetes is supported.

Key features include:
- **NFS Management**: Provision and manage NFS servers for shared storage.
- **Folder Management**: Create, update, and delete storage folders on disks.
- **Disk Management**: Probe, format, mount, and manage storage disks.
- **Block Storage Management**: Provision, format, mount, and manage block storage devices with support for replicas and snapshots.
- **Configuration Management**: Centralized configuration for clusters, namespaces, and other parameters.

### Requirements

These services require the following services to be running:

- [Kubernetes-service](https://github.com/FLYBYME/kubernetes-service)
- [Config-service](https://github.com/FLYBYME/config-service)
- [Nodes-service](https://github.com/FLYBYME/nodes-service)

## Services

### NFS Service

Manages NFS servers. NFS servers are used to provide shared storage for storage folders.

#### Actions

- `createNFS`: Creates a new NFS server. Requires a disk ID as a parameter. It checks if the disk exists and if it already has an NFS server. If not, it creates a new NFS server.
- `createPod`: Creates a Kubernetes pod for the NFS server. Requires the NFS server ID as a parameter. It checks if the NFS server exists and if it already has a pod. If not, it creates a new pod.
- `deletePod`: Deletes the Kubernetes pod for the NFS server. Requires the NFS server ID as a parameter. It checks if the NFS server exists and if it has a pod. If so, it deletes the pod.
- `getPod`: Retrieves the Kubernetes pod for the NFS server. Requires the NFS server ID as a parameter. It checks if the NFS server exists and retrieves the pod information.

#### Methods

- `resolveNFS`: Resolves an NFS server by its ID.
- `resolveNFSPod`: Resolves an NFS pod by its UID.
- `createNFS`: Creates a new NFS server on a node. It generates a unique name, constructs the FQDN, and determines the mount path. It then creates the NFS server entity and updates the disk with the NFS server information.
- `createNFSPod`: Creates a Kubernetes pod for the NFS server. It checks if the NFS server already has a pod. If not, it creates a new pod with the necessary configurations and updates the NFS server entity with the pod information.
- `deleteNFSPod`: Deletes the Kubernetes pod for the NFS server. It checks if the NFS server has a pod. If so, it deletes the pod and updates the NFS server entity to remove the pod information.

### Folders Service

Manages storage folders. Folders are used as mount points for the block devices and storage for replicas.

#### Actions

- `provision`: Provisions a new folder. Requires a prefix for the folder name and a disk ID as parameters. It checks if the disk exists and then creates a new folder on the disk.
- `deprovision`: Deprovisions an existing folder. Requires the folder ID as a parameter. It checks if the folder exists and then removes it from the disk.
- `updateUsed`: Updates the used size of a folder. Requires the folder ID as a parameter. It checks if the folder exists and then updates its used size.
- `updateAllUsed`: Updates the used size of all folders. This action is typically used as a cron job to periodically update the used sizes of all folders.

#### Methods

- `findByID`: Finds a folder by its ID.
- `provisionFolder`: Provisions a new folder on a disk. It generates a unique folder name, determines the folder path, creates the folder on the disk, and then creates the folder entity in the database.
- `deprovisionFolder`: Deprovisions a folder. It checks if the disk exists, determines the folder path, removes the folder from the disk, and then removes the folder entity from the database.
- `getUsed`: Gets the used size of a folder. It calls the `getFolderSize` action of the `disks` service to retrieve the size of the folder and returns the used size.

### Disks Service

Manages storage disks.

#### Actions

- `availableDisks`: Retrieves available disks.
- `probe`: Probes a node for storage devices.
- `probeAll`: Probes all nodes for storage devices.
- `updateUsed`: Updates the used size of a disk.
- `updateAllUsed`: Updates the used size of all disks.
- `mountPoint`: Retrieves the mount point of a disk.
- `setNFS`: Sets the NFS server for a disk.
- `createFolder`: Creates a folder on a disk.
- `removeFolder`: Removes a folder from a disk.
- `getFolderSize`: Retrieves the size of a folder.
- `nfs`: Resolves the NFS server for a folder on a disk.
- `getNodeDisks`: Retrieves disks for a specific node.

### Blocks Service

Manages block storage devices.

#### Provisioning

- `provision`: Provisions a new block storage device.
  - **Params**:
    - `name` (string): Name of the block.
    - `size` (number): Size of the block in GB.
    - `node` (string): Node to create the block.
    - `replicas` (number): Number of replicas.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.provision", {
      name: "my-block",
      size: 10,
      node: "node-1",
      replicas: 3
    });
    ```

- `deprovision`: Deprovisions an existing block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.deprovision", {
      id: "block-id"
    });
    ```

#### Formatting and Mounting

- `format`: Formats a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `force` (boolean, optional): Force format.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.format", {
      id: "block-id",
      force: true
    });
    ```

- `mount`: Mounts a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `force` (boolean, optional): Force mount.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.mount", {
      id: "block-id",
      force: true
    });
    ```

- `unmount`: Unmounts a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `force` (boolean, optional): Force unmount.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.unmount", {
      id: "block-id",
      force: true
    });
    ```

#### Usage and Maintenance

- `usage`: Retrieves the usage of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    const usage = await broker.call("storage.blocks.usage", {
      id: "block-id"
    });
    console.log(usage);
    ```

- `trim`: Trims a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.trim", {
      id: "block-id"
    });
    ```

- `checkPods`: Checks the state of the pods associated with a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.checkPods", {
      id: "block-id"
    });
    ```

- `balanceBlock`: Balances the number of replicas of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.balanceBlock", {
      id: "block-id"
    });
    ```

#### Replica Management

- `createReplica`: Creates a new replica for a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `disk` (string): Disk ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.createReplica", {
      id: "block-id",
      disk: "disk-id"
    });
    ```

- `removeReplicaFromBlock`: Removes a replica from a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.removeReplicaFromBlock", {
      id: "block-id",
      replica: "replica-id"
    });
    ```

- `removeReplicaFromFrontend`: Removes a replica from a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.removeReplicaFromFrontend", {
      id: "block-id",
      replica: "replica-id"
    });
    ```

- `removeReplicaPod`: Removes a replica pod from a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.removeReplicaPod", {
      id: "block-id",
      replica: "replica-id"
    });
    ```

- `addReplicaToFrontend`: Adds a replica to a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.addReplicaToFrontend", {
      id: "block-id",
      replica: "replica-id"
    });
    ```

- `listReplicas`: Lists replicas of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    const replicas = await broker.call("storage.blocks.listReplicas", {
      id: "block-id"
    });
    console.log(replicas);
    ```

- `updateReplica`: Updates a replica of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
    - `mode` (string): Replica mode.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.updateReplica", {
      id: "block-id",
      replica: "replica-id",
      mode: "RW"
    });
    ```

- `getRebuildStatus`: Gets the rebuild status of a replica of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    const status = await broker.call("storage.blocks.getRebuildStatus", {
      id: "block-id",
      replica: "replica-id"
    });
    console.log(status);
    ```

- `verifyRebuild`: Verifies the rebuild status of a replica of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `replica` (string): Replica ID.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.verifyRebuild", {
      id: "block-id",
      replica: "replica-id"
    });
    ```

- `updateReplicaMode`: Updates the mode of a replica of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
    - `mode` (string): Replica mode.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.updateReplicaMode", {
      id: "block-id",
      mode: "RW"
    });
    ```

#### Information and Control

- `ls`: Lists the contents of a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    const contents = await broker.call("storage.blocks.ls", {
      id: "block-id"
    });
    console.log(contents);
    ```

- `info`: Retrieves information about a block storage device.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    const info = await broker.call("storage.blocks.info", {
      id: "block-id"
    });
    console.log(info);
    ```

#### Longhorn Block Controller

- `createBlockController`: Creates a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.createBlockController", {
      id: "block-id"
    });
    ```

- `startBlockFrontend`: Starts a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.startBlockFrontend", {
      id: "block-id"
    });
    ```

- `shutdownBlockFrontend`: Shuts down a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.shutdownBlockFrontend", {
      id: "block-id"
    });
    ```

- `getBlockControllerInfo`: Retrieves information about a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    const info = await broker.call("storage.blocks.getBlockControllerInfo", {
      id: "block-id"
    });
    console.log(info);
    ```

- `expandBlockController`: Expands a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
    - `size` (number): New block size.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.expandBlockController", {
      id: "block-id",
      size: 20
    });
    ```

- `deleteBlockController`: Deletes a Longhorn block controller.
  - **Params**:
    - `id` (string): ID of the block.
  - **Example**:
    ```javascript
    await broker.call("storage.blocks.deleteBlockController", {
      id: "block-id"
    });
    ```

## Configuration

The services use configuration settings defined in the `config` object within each service file. These settings include default values for clusters, namespaces, and other parameters.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/FLYBYME/block-storage.git
   cd block-storage
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the services:
   ```bash
   npm run dev
   ```

## License

This project is licensed under the MIT License.
