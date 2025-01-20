
const { MoleculerClientError } = require("moleculer").Errors;
const { v4: uuid } = require("uuid");
const Moniker = require("moniker");

/**
 * longhorn block replicas mixin
 * Moleculer service mixin for Longhorn block replicas management
 * 
 */

// NAME:
//    longhorn add-replica -
// USAGE:
//    longhorn add-replica [command options] [arguments...]
// OPTIONS:
//    --restore                              Set this flag if the replica is being added to a restore/DR volume
//    --size value                           Volume nominal size in bytes or human readable 42kb, 42mb, 42gb
//    --current-size value                   Volume current size in bytes or human readable 42kb, 42mb, 42gb
//    --fast-sync                            Enable fast file synchronization using change time and checksum
//    --file-sync-http-client-timeout value  HTTP client timeout for replica file sync server (default: 5)
//    --replica-instance-name value          Name of the replica instance (for validation purposes)

// NAME:
//    longhorn ls-replica -
// USAGE:
//    longhorn ls-replica [arguments...]


// NAME:
//    longhorn rm-replica -
// USAGE:
//    longhorn rm-replica [arguments...]


// NAME:
//    longhorn update-replica -
// USAGE:
//    longhorn update-replica [command options] [arguments...]
// OPTIONS:
//    --mode value  Replica mode. The value can be RO, RW or ERR.


// NAME:
//    longhorn replica-rebuild-status -
// USAGE:
//    longhorn replica-rebuild-status [arguments...]

// NAME:
//    longhorn verify-rebuild-replica -
// USAGE:
//    longhorn verify-rebuild-replica [command options] [arguments...]
// OPTIONS:
//    --replica-instance-name value  Name of the replica instance (for validation purposes)

// NAME:
//    longhorn replica -
// USAGE:
//    longhorn replica DIRECTORY
// OPTIONS:
//    --listen value                   (default: "localhost:9502")
//    --backing-file value             qcow file or encapsulating directory to use as the base image of this disk
//    --sync-agent
//    --size value                     Volume size in bytes or human readable 42kb, 42mb, 42gb
//    --restore-from value             specify backup to be restored, must be used with --restore-name
//    --restore-name value             specify the snapshot name for restore, must be used with --restore-from
//    --sync-agent-port-count value    (default: 10)
//    --disableRevCounter              To disable revision counter for every write
//    --data-server-protocol value     Specify the data-server protocol. Available options are "tcp" and "unix" (default: "tcp")
//    --unmap-mark-disk-chain-removed  To mark the current disk chain as removed before starting unmap
//    --replica-instance-name value    Name of the replica instance (for validation purposes)
//    --snapshot-max-count value       Maximum number of snapshots to keep (default: 250)
//    --snapshot-max-size value        Maximum total snapshot size in bytes or human readable 42kb, 42mb, 42gb

module.exports = {
    name: "storage.blocks.replicas",
    settings: {

    },
    actions: {
        /**
         * Create new replica for a block
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} disk - disk id
         * 
         * @returns {Object} - updated block object
         */
        createReplica: {
            rest: {
                method: "POST",
                path: "/:id/replicas/create"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                disk: {
                    type: "string",
                    empty: false,
                    optional: false
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const disk = await ctx.call("v1.storage.disks.resolve", { id: ctx.params.disk });
                if (!disk) {
                    throw new MoleculerClientError(
                        `Disk with id '${ctx.params.disk}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.disk }
                    );
                }

                return this.createReplica(ctx, block, disk);
            }
        },

        /**
         * Remove a replica from a block
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id
         * 
         * @returns {Object} - updated block object
         */
        removeReplicaFromBlock: {
            rest: {
                method: "POST",
                path: "/:id/replicas/:replica/remove"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                return this.removeReplicaFromBlock(ctx, block, replica);
            }
        },

        /**
         * Add a replica to a volume frontend
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id to add
         * @param {Boolean} restore - restore flag
         * @param {Boolean} fastSync - fast sync flag
         * @param {Number} fileSyncHttpClientTimeout - file sync http client timeout
         * 
         * @returns {Object} - result from the command
         */
        addReplicaToFrontend: {
            rest: {
                method: "POST",
                path: "/:id/replicas/add"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                restore: {
                    type: "boolean",
                    optional: true
                },
                fastSync: {
                    type: "boolean",
                    optional: true
                },
                fileSyncHttpClientTimeout: {
                    type: "number",
                    optional: true
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                return this.addReplicaToFrontend(ctx, block, replica, {
                    restore: ctx.params.restore,
                    fastSync: ctx.params.fastSync,
                    fileSyncHttpClientTimeout: ctx.params.fileSyncHttpClientTimeout
                });
            }
        },

        /**
         * List replicas of a block
         * 
         * @actions
         * @param {String} id - block storage id
         * 
         * @returns {Array} - replicas
         */
        listReplicas: {
            rest: {
                method: "GET",
                path: "/:id/replicas"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                return this.listReplicas(ctx, block);
            }
        },

        /**
         * Remove a replica from a block frontend
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id
         * 
         * @returns {Object} - result from the command
         */
        removeReplicaFromFrontend: {
            rest: {
                method: "DELETE",
                path: "/:id/replicas/:replica"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                force: {
                    type: "boolean",
                    optional: true,
                    default: false,
                    convert: true
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                if (block.replicas.length === 1 && !ctx.params.force) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} only has one replica and force flag is not set`,
                        400, "BAD_REQUEST", { id: ctx.params.replica }
                    );
                }

                return this.removeReplicaFromFrontend(ctx, block, replica);
            }
        },

        /**
         * Update a replica of a block
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id
         * @param {String} mode - replica mode
         * 
         * @returns {Object} - result from the command
         */
        updateReplica: {
            rest: {
                method: "POST",
                path: "/:id/replicas/:replica/update"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                mode: {
                    type: "string",
                    empty: false,
                    enum: ["RO", "RW", "ERR"],
                    optional: false
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                return this.updateReplica(ctx, block, replica, {
                    mode: ctx.params.mode
                });
            }
        },

        /**
         * Get the rebuild status of a replica of a block
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id
         * 
         * @returns {Object} - result from the command
         */
        getRebuildStatus: {
            rest: {
                method: "GET",
                path: "/:id/replicas/:replica/rebuild-status"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                }
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                return this.getRebuildStatus(ctx, block, replica);
            }
        },

        /**
         * Verify the rebuild status of a replica of a block
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica id
         * @param {String} instance - replica instance name
         * 
         * @returns {Object} - result from the command
         */
        verifyRebuild: {
            rest: {
                method: "POST",
                path: "/:id/replicas/:replica/verify-rebuild"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                replica: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    throw new MoleculerClientError(
                        `Block storage with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                const replica = block.replicas.find(r => r.id === ctx.params.replica);
                if (!replica) {
                    throw new MoleculerClientError(
                        `Block storage ${block.id} replica ${ctx.params.replica} not found`,
                        404, "NOT_FOUND", { id: ctx.params.replica }
                    );
                }

                return this.verifyRebuild(ctx, block, replica);
            }
        },
    },
    methods: {
        /**
         * Create new replica object
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} disk - Disk object where the replica is created
         * 
         * @returns {Object} - Updated block object
         */
        async createReplica(ctx, block, disk) {

            const node = await ctx.call("v1.nodes.resolve", { id: disk.node });
            if (!node) {
                throw new MoleculerClientError(
                    `Node with id '${disk.node}' not found`,
                    404, "NOT_FOUND", { id: disk.node }
                );
            }

            const folder = await ctx.call("v1.storage.folders.provision", {
                prefix: "block-replica",
                disk: disk.id
            });

            const name = `block-replica-${block.name}-${Moniker.choose()}`;

            const config = {
                id: uuid(),
                pod: null,
                name,
                disk: disk.id,
                node: node.id,
                folder: folder.id,
                healthy: false,
                attached: false,
                status: "pending",
                ip: null,
                endpoint: null,
            };

            const replica = await this.createReplicaPod(ctx, block, config, folder);

            const updated = await this.updateEntity(ctx, {
                id: block.id,
                replicas: [...block.replicas, replica]
            });

            return updated;
        },

        /**
         * Create new replica pod
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * @param {Object} folder - Folder object
         * 
         * @returns {Object} - Updated block object
         */
        async createReplicaPod(ctx, block, replica, folder) {


            const node = await ctx.call("v1.nodes.resolve", { id: replica.node });

            const volumeMounts = [{
                name: 'node-mount',
                mountPath: '/mnt',
            }];
            const ports = [];

            const portCount = 15;
            const startPort = 10000;

            // create 15 ports
            for (let i = 0; i < portCount; i++) {
                ports.push({
                    name: `replica-${i}`,
                    containerPort: startPort + i,
                    protocol: 'TCP'
                });
            }

            const command = [
                `longhorn`,
                `replica`,
                `/mnt/`,
                `--size`,
                `${block.size}gb`,
                `--replica-instance-name`,
                replica.name,
                `--listen`,
                `0.0.0.0:${startPort}`,
                `--data-server-protocol`,
                `tcp`,
                `--snapshot-max-count`,
                `250`,
                `--snapshot-max-size`,
                `1gb`
            ];

            // create a new replica pod
            const pod = await ctx.call("v1.kubernetes.createNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                body: {
                    apiVersion: "v1",
                    kind: "Pod",
                    metadata: {
                        name: replica.name,
                        labels: {
                            "moleculer": "true",
                            "longhorn.io": "true",
                            "longhorn-replica": "true",
                            "block-replica": "true",
                            "block": block.id,
                            "replica": replica.id,
                            "folder": folder.id
                        }
                    },
                    spec: {
                        nodeName: node.hostname,
                        restartPolicy: "Never",
                        containers: [{
                            name: "block-replica",
                            image: this.config.get("storage.blocks.engineImage"),
                            volumeMounts: volumeMounts,
                            ports: ports,
                            command: command,
                        }],
                        volumes: [{
                            name: "node-mount",
                            hostPath: {
                                path: folder.path
                            }
                        }]
                    }
                },
            });

            if (!pod) {
                // remove the folder
                await ctx.call("v1.storage.folders.deprovision", { id: folder.id });
                throw new MoleculerClientError(
                    `Error creating pod ${name}`,
                    500, "POD_CREATION_ERROR", { name }
                );
            }

            replica.pod = pod.metadata.uid;

            return replica;
        },

        async removeReplicaPod(ctx, block, replica) {
            const updated = await this.removeReplicaFromFrontend(ctx, block, replica);

            await ctx.call("v1.kubernetes.deleteNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: replica.name
            });

            return updated;
        },

        /**
         * Remove a replica from a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async removeReplicaFromBlock(ctx, block, replica) {

            let updated = await this.removeReplicaFromFrontend(ctx, block, replica)
                .catch(() => {
                    this.logger.error(`Failed to remove replica ${replica.id} from block ${block.id}`);
                });

            await ctx.call("v1.kubernetes.deleteNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: replica.name
            }).catch(() => {
                this.logger.error(`Failed to delete pod ${replica.name}`);
            });

            await ctx.call("v1.storage.folders.deprovision", { id: replica.folder })
                .catch(() => {
                    this.logger.error(`Failed to deprovision folder ${replica.folder}`);
                });

            updated = await this.updateEntity(ctx, {
                id: block.id,
                replicas: updated.replicas.filter(r => r.id !== replica.id)
            });

            this.logger.info(`Removed replica ${replica.id} from block ${block.id}`);

            return this.updateFrontendState(ctx, updated);
        },

        /**
         * Add a replica to a volume
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * @param {Object} options - Options object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async addReplicaToFrontend(ctx, block, replica, options = {}) {
            if (!replica.healthy) {
                this.logger.info(`Block storage ${block.id} replica pod ${replica.id} is not healthy`);
                return;
            } else if (!block.online) {
                this.logger.info(`Block storage ${block.id} is not online`);
                return;
            }

            const cmd = [
                "longhorn",
                "add-replica",
                "--replica-instance-name",
                replica.name,
                "--size",
                `${block.size}gb`,
                "--current-size",
                `${block.size}gb`,
            ];

            if (options.restore) {
                cmd.push("--restore");
            }

            if (options.fastSync) {
                cmd.push("--fast-sync");
            }

            if (options.fileSyncHttpClientTimeout) {
                cmd.push("--file-sync-http-client-timeout", options.fileSyncHttpClientTimeout);
            }

            cmd.push(replica.endpoint);

            const result = await this.exec(ctx, block, cmd);

            if (result.stderr.includes("Error running add replica command")) {
                throw new MoleculerClientError(
                    result.stderr,
                    500, "LONGHORN_ADD_REPLICA_ERROR", { command: cmd }
                );
            }

            replica.attached = true;

            const updated = this.updateEntity(ctx, {
                id: block.id,
                replicas: block.replicas
            });

            return this.updateFrontendState(ctx, updated);
        },

        /**
         * List replicas of a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async listReplicas(ctx, block) {

            const cmd = [
                "longhorn",
                "ls-replica",
            ];

            const result = await this.exec(ctx, block, cmd);

            const lines = result.stdout.split("\n");
            const replicas = lines.slice(1)
                .filter(line => line.length > 0)
                .map(line => {
                    const parts = line.split(" ").filter(p => p.length > 0);
                    // tcp://10.42.0.18:10000 RW   [volume-head-001.img volume-snap-16e36ddf-7924-4409-994e-982f714a1c26.img]
                    const endpoint = parts.shift();
                    const mode = parts.shift();

                    const replica = block.replicas.find(r => r.endpoint === endpoint);

                    const volumeRegex = /\[(.*)\]/;
                    const volumeMatch = line.match(volumeRegex);
                    let volumes = [];

                    if (volumeMatch) {
                        volumes = volumeMatch[1].split(" ");
                    }

                    return {
                        ...replica,
                        endpoint,
                        mode,
                        volumes
                    };
                });

            return replicas;
        },

        /**
         * Remove a replica from a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async removeReplicaFromFrontend(ctx, block, replica) {

            if (!replica.endpoint) {
                throw new MoleculerClientError(
                    `Replica ${replica.id} has no endpoint`,
                    500, "NO_REPLICA_ENDPOINT", { id: block.id }
                );
            }

            const cmd = [
                "longhorn",
                "rm-replica",
                replica.endpoint,
            ];

            const result = await this.exec(ctx, block, cmd);

            if (result.stderr.includes(" cannot remove last replica if volume is up")) {
                throw new MoleculerClientError(
                    `Cannot remove last replica if volume is up`,
                    500, "CANNOT_REMOVE_LAST_REPLICA", { id: block.id }
                );
            }

            replica.attached = false;

            const updated = this.updateEntity(ctx, {
                id: block.id,
                replicas: block.replicas
            });

            return this.updateFrontendState(ctx, updated);  
        },

        /**
         * Update a replica of a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * @param {Object} options - Options object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async updateReplica(ctx, block, replica, options = {}) {

            const cmd = [
                "longhorn",
                "update-replica",
                "--mode",
                options.mode,
                replica.endpoint,
            ];

            const result = await this.exec(ctx, block, cmd);

            return result;
        },

        /**
         * Get the rebuild status of a replica of a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async getRebuildStatus(ctx, block, replica) {

            const cmd = [
                "longhorn",
                "replica-rebuild-status",
                replica.endpoint,
            ];

            const result = await this.exec(ctx, block, cmd);

            return result;
        },

        /**
         * Verify the rebuild status of a replica of a block
         * 
         * @param {Context} ctx - Moleculer context object
         * @param {Object} block - Block object
         * @param {Object} replica - Replica object
         * @param {Object} options - Options object
         * 
         * @returns {Promise<Object>} - Replica object
         */
        async verifyRebuild(ctx, block, replica, options = {}) {

            const cmd = [
                "longhorn",
                "verify-rebuild-replica",
                "--replica-instance-name",
                replica.id,
                replica.endpoint,
            ];

            const result = await this.exec(ctx, block, cmd);

            return result;
        },
    },
};