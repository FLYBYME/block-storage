
# Block Service Documentation

## Overview

The Block Service is responsible for managing block storage devices. It provides functionalities to provision, deprovision, format, mount, unmount, and manage block storage devices. Additionally, it supports replica management, snapshot operations, and integration with Longhorn for block storage orchestration.

## Actions

### Provision

Provisions a new block storage device.

- **Endpoint**: `POST /v1/storage/blocks/provision`
- **Params**:
  - `name` (string): Name of the block.
  - `size` (number): Size of the block in GB.
  - `node` (string): Node to create the block.
  - `replicas` (number): Number of replicas.
- **Returns**: The created block object.

### Deprovision

Deprovisions an existing block storage device.

- **Endpoint**: `DELETE /v1/storage/blocks/:id/deprovision`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The ID of the deleted block.

### Format

Formats a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/format`
- **Params**:
  - `id` (string): ID of the block.
  - `force` (boolean, optional): Force format.
- **Returns**: The updated block object.

### Mount

Mounts a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/mount`
- **Params**:
  - `id` (string): ID of the block.
  - `force` (boolean, optional): Force mount.
- **Returns**: The updated block object.

### Unmount

Unmounts a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/unmount`
- **Params**:
  - `id` (string): ID of the block.
  - `force` (boolean, optional): Force unmount.
- **Returns**: The updated block object.

### Usage

Retrieves the usage of a block storage device.

- **Endpoint**: `GET /v1/storage/blocks/:id/usage`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block usage.

### Trim

Trims a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/trim`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block object.

### Check Pods

Checks the state of the pods associated with a block storage device.

- **Endpoint**: `GET /v1/storage/blocks/:id/check-pods`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block object.

### Balance Block

Balances the number of replicas of a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/balance`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block object.

### List Replicas

Lists replicas of a block storage device.

- **Endpoint**: `GET /v1/storage/blocks/:id/replicas`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: Array of replicas.

### Create Replica

Creates a new replica for a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/replicas/create`
- **Params**:
  - `id` (string): ID of the block.
  - `disk` (string): Disk ID.
- **Returns**: The updated block object.

### Remove Replica from Block

Removes a replica from a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/replicas/:replica/remove`
- **Params**:
  - `id` (string): ID of the block.
  - `replica` (string): Replica ID.
- **Returns**: The updated block object.

### Add Replica to Frontend

Adds a replica to a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/replicas/add`
- **Params**:
  - `id` (string): ID of the block.
  - `replica` (string): Replica ID.
  - `restore` (boolean, optional): Restore flag.
  - `fastSync` (boolean, optional): Fast sync flag.
  - `fileSyncHttpClientTimeout` (number, optional): File sync HTTP client timeout.
- **Returns**: The result from the command.

### Remove Replica from Frontend

Removes a replica from the frontend controller.

- **Endpoint**: `DELETE /v1/storage/blocks/:id/replicas/:replica`
- **Params**:
  - `id` (string): ID of the block.
  - `replica` (string): Replica ID.
  - `force` (boolean, optional): Force flag.
- **Returns**: The result from the command.

### Get Rebuild Status

Gets the rebuild status of a replica of a block storage device.

- **Endpoint**: `GET /v1/storage/blocks/:id/replicas/:replica/rebuild-status`
- **Params**:
  - `id` (string): ID of the block.
  - `replica` (string): Replica ID.
- **Returns**: The result from the command.

### Verify Rebuild

Verifies the rebuild status of a replica of a block storage device.

- **Endpoint**: `POST /v1/storage/blocks/:id/replicas/:replica/verify-rebuild`
- **Params**:
  - `id` (string): ID of the block.
  - `replica` (string): Replica ID.
- **Returns**: The result from the command.

### Create Snapshot

Creates a snapshot of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/create`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block object.

### Revert Snapshot

Reverts a volume to a specific snapshot.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/revert`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.

### List Snapshots

Lists snapshots of a volume.

- **Endpoint**: `GET /v1/storage/blocks/:id/snapshots/list`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: Array of snapshot objects.

### Remove Snapshot

Removes a snapshot of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/remove`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.

### Purge Snapshots

Purges snapshots of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/purge`
- **Params**:
  - `id` (string): ID of the block.
  - `skipInProgress` (boolean, optional): Skip in-progress flag.
- **Returns**: The block object.

### Purge Snapshots Status

Gets the status of the purge operation of snapshots of a volume.

- **Endpoint**: `GET /v1/storage/blocks/:id/snapshots/purge-status`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: The block object.

### Get Snapshot Info

Gets information about a snapshot of a volume.

- **Endpoint**: `GET /v1/storage/blocks/:id/snapshots/info`
- **Params**:
  - `id` (string): ID of the block.
- **Returns**: Snapshot object.

### Clone Snapshot

Clones a snapshot of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/clone`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
  - `fromControllerAddress` (string): Address of the engine controller of the source volume.
  - `fromVolumeName` (string): Name of the source volume.
  - `fromControllerInstanceName` (string): Name of the engine controller instance of the source volume.
- **Returns**: The block object.

### Clone Snapshot Status

Gets the status of the clone operation of a snapshot of a volume.

- **Endpoint**: `GET /v1/storage/blocks/:id/snapshots/clone-status`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.

### Start Snapshot Hash

Starts the hash calculation of a snapshot of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/hash`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.

### Cancel Snapshot Hash

Cancels the hash calculation of a snapshot of a volume.

- **Endpoint**: `POST /v1/storage/blocks/:id/snapshots/hash-cancel`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.

### Get Snapshot Hash Status

Gets the status of the hash calculation of a snapshot of a volume.

- **Endpoint**: `GET /v1/storage/blocks/:id/snapshots/hash-status`
- **Params**:
  - `id` (string): ID of the block.
  - `snapshotName` (string): Name of the snapshot.
- **Returns**: The block object.
