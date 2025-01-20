const { MoleculerClientError } = require("moleculer").Errors;

/**
 * Longhorn block controller mixin
 * Moleculer service mixin for managing Longhorn block controllers
 */

// NAME:
//    longhorn controller -
// USAGE:
//    longhorn controller [command options] [arguments...]
// OPTIONS:
//    --listen value                         (default: "localhost:9501")
//    --size value                           Volume nominal size in bytes or human readable 42kb, 42mb, 42gb
//    --current-size value                   Volume current size in bytes or human readable 42kb, 42mb, 42gb
//    --frontend value
//    --enable-backend value                 (default: "tcp")
//    --replica value
//    --upgrade
//    --disableRevCounter                    To disable revision counter checking
//    --salvageRequested                     Start engine controller in a special mode only to get best replica candidate for salvage
//    --engine-replica-timeout value         In seconds. Timeout between engine and replica(s) (default: 8)
//    --data-server-protocol value           Specify the data-server protocol. Available options are "tcp" and "unix" (default: "tcp")
//    --unmap-mark-snap-chain-removed        To enable marking snapshot chain as removed during unmap
//    --file-sync-http-client-timeout value  HTTP client timeout for replica file sync server (default: 5)
//    --snapshot-max-count value             Maximum number of snapshots to keep (default: 250)
//    --snapshot-max-size value              Maximum total snapshot size in bytes or human readable 42kb, 42mb, 42gb

// NAME:
//    longhorn start-with-replicas -
// USAGE:
//    longhorn start-with-replicas [command options] [arguments...]
// OPTIONS:
//    --size value          Volume nominal size in bytes or human readable 42kb, 42mb, 42gb
//    --current-size value  Volume current size in bytes or human readable 42kb, 42mb, 42gb


// NAME:
//    longhorn info -
// USAGE:
//    longhorn info [arguments...]

// NAME:
//    longhorn expand -
// USAGE:
//    longhorn expand [command options] [arguments...]
// OPTIONS:
//    --size value  The new volume size. It should be larger than the current size (default: 0)


// NAME:
//    longhorn frontend -
// USAGE:
//    longhorn frontend command [command options] [arguments...]
// COMMANDS:
//    start     start <frontend name>
//    shutdown  shutdown
// OPTIONS:
//    --help, -h  show help


module.exports = {
    name: "storage.block.controllers",
    settings: {},
    actions: {
        /**
         * Create a Longhorn block controller
         * 
         * @actions
         * @param {String} id - Block ID
         * 
         * @returns {Object} - Updated block controller
         */
        createBlockController: {
            rest: {
                method: "POST",
                path: "/:id/controller"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
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

                return this.createBlockController(ctx, block);
            }
        },

        /**
         * Start a Longhorn block controller
         * 
         * @actions
         * @param {String} id - Block ID
         * 
         * @returns {Object} - Updated block controller
         */
        startBlockFrontend: {
            rest: {
                method: "POST",
                path: "/:id/controller/start"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
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

                return this.startBlockFrontend(ctx, block);
            }
        },

        /**
         * Shutdown a Longhorn block controller
         * 
         * @actions
         * @param {String} id - Block ID
         * 
         * @returns {Object} - Updated block controller
         */
        shutdownBlockFrontend: {
            rest: {
                method: "POST",
                path: "/:id/controller/shutdown"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
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

                return this.shutdownBlockFrontend(ctx, block);
            }
        },

        /**
         * Get Longhorn block controller info
         * 
         * @actions
         * @param {String} id - Block ID
         * 
         * @returns {Object} - Block controller info
         */
        getBlockControllerInfo: {
            rest: {
                method: "GET",
                path: "/:id/controller/info"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
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

                return this.getBlockControllerInfo(ctx, block);
            }
        },

        /**
         * Expand a Longhorn block controller
         * 
         * @actions
         * @param {String} id - Block ID
         * @param {Number} size - New block size
         * 
         * @returns {Object} - Updated block controller
         */
        expandBlockController: {
            rest: {
                method: "POST",
                path: "/:id/controller/expand"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
                },
                size: {
                    type: "number",
                    min: 1,
                    optional: false,
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

                return this.expandBlockController(ctx, block, { size: ctx.params.size });
            }
        },

        /**
         * Delete a Longhorn block controller
         * 
         * @actions
         * @param {String} id - Block ID
         * 
         * @returns {Object} - Updated block controller
         */
        deleteBlockController: {
            rest: {
                method: "DELETE",
                path: "/:id/controller"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false,
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

                return this.deleteBlockController(ctx, block);
            }
        }
    },
    methods: {
        /**
         * Create a Longhorn block controller
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * @params {Object} options - Action options
         * 
         * @returns {Promise<Object>} - Updated block controller
         */
        async createBlockController(ctx, block, options = {}) {
            if (block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} already exists`,
                    409, "BLOCK_CONTROLLER_EXISTS", { controller: block.controller }
                );
            }

            const node = await ctx.call("v1.nodes.resolve", { id: block.node });
            if (!node) {
                throw new MoleculerClientError(
                    `Node ${block.node} not found`,
                    404, "NODE_NOT_FOUND", { node: block.node }
                );
            }

            const command = [
                "longhorn",
                "controller",
                "--listen",
                "0.0.0.0:9501",
                "--size",
                `${block.size}gb`,
                "--current-size",
                `${block.size}gb`,
                "--frontend",
                this.config.get('storage.blocks.frontend'),
            ];

            for (const replica of block.replicas) {
                command.push("--replica");
                command.push(replica.endpoint);
            }
            if (options.upgrade) {
                command.push("--upgrade");
            }
            if (options.disableRevCounter) {
                command.push("--disableRevCounter");
            }
            if (options.salvageRequested) {
                command.push("--salvageRequested");
            }
            if (options.unmapMarkSnapChainRemoved) {
                command.push("--unmap-mark-snap-chain-removed");
            }

            if (options.snapshotMaxCount) {
                command.push("--snapshot-max-count");
                command.push(options.snapshotMaxCount);
            }

            if (options.snapshotMaxSize) {
                command.push("--snapshot-max-size");
                command.push(options.snapshotMaxSize);
            }

            if (options.engineReplicaTimeout) {
                command.push("--engine-replica-timeout");
                command.push(options.engineReplicaTimeout);
            }

            if (options.dataServerProtocol) {
                command.push("--data-server-protocol");
                command.push(options.dataServerProtocol);
            }

            if (options.fileSyncHttpClientTimeout) {
                command.push("--file-sync-http-client-timeout");
                command.push(options.fileSyncHttpClientTimeout);
            }

            command.push(block.name);

            const pod = await ctx.call("v1.kubernetes.createNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: block.name,
                body: {
                    metadata: {
                        name: block.name,
                        namespace: block.namespace,
                        labels: {
                            "moleculer": "true",
                            "longhorn.io": "true",
                            "longhorn-controller": "true",
                            "block": block.id
                        }
                    },
                    spec: {
                        containers: [{
                            name: block.name,
                            image: this.config.get('storage.blocks.engineImage'),
                            command: command,
                            ports: [{
                                name: 'controller',
                                containerPort: 9501,
                                protocol: 'TCP'
                            }],
                            securityContext: {
                                privileged: true
                            },
                            volumeMounts: [{
                                name: 'node-mount',
                                mountPath: '/mnt',
                            }, {
                                name: 'dev',
                                mountPath: '/host/dev',
                            }, {
                                name: 'proc',
                                mountPath: '/host/proc',
                            }]
                        }],
                        nodeName: node.hostname,
                        volumes: [{
                            name: 'node-mount',
                            hostPath: {
                                path: '/mnt'
                            }
                        }, {
                            name: 'dev',
                            hostPath: {
                                path: '/dev'
                            }
                        }, {
                            name: 'proc',
                            hostPath: {
                                path: '/proc'
                            }
                        }],
                    }
                }
            })

            const updated = await this.updateEntity(ctx, {
                id: block.id,
                controller: pod.metadata.uid,
            });

            this.logger.info(`Created block controller ${pod.metadata.uid}`);

            return updated;
        },

        /**
         * Start a Longhorn block controller
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Object>} - Updated block controller
         */
        async startBlockFrontend(ctx, block) {
            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} not found`,
                    404, "BLOCK_CONTROLLER_NOT_FOUND", { controller: block.controller }
                );
            }

            const command = [
                "longhorn",
                "frontend",
                "start",
                this.config.get('storage.blocks.frontend'),
            ];

            const result = await this.exec(ctx, block, command);

            this.logger.info(`Started block controller ${block.controller} with result: ${JSON.stringify(result)}`);

            await this.updateFrontendState(ctx, block);

            return result;
        },

        /**
         * Shutdown a Longhorn block controller
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Object>} - Updated block controller
         */
        async shutdownBlockFrontend(ctx, block) {
            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} not found`,
                    404, "BLOCK_CONTROLLER_NOT_FOUND", { controller: block.controller }
                );
            }

            const command = [
                "longhorn",
                "frontend",
                "shutdown",
            ];

            const result = await this.exec(ctx, block, command);

            this.logger.info(`Shutdown block controller ${block.controller} with result: ${JSON.stringify(result)}`);

            await this.updateFrontendState(ctx, block);

            return result;
        },

        /**
         * Get Longhorn block controller info
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * @params {Object} options - Action options
         * 
         * @returns {Promise<Object>} - Block controller info
         */
        async getBlockControllerInfo(ctx, block, options = {}) {
            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} not found`,
                    404, "BLOCK_CONTROLLER_NOT_FOUND", { controller: block.controller }
                );
            }

            const command = [
                "longhorn",
                "info",
            ];

            const result = await this.exec(ctx, block, command);

            const json = JSON.parse(result.stdout);

            return json;
        },

        /**
         * Expand a Longhorn block controller
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * @params {Object} options - Action options
         * 
         * @returns {Promise<Object>} - Updated block controller
         */
        async expandBlockController(ctx, block, options = {}) {

            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} not found`,
                    404, "BLOCK_CONTROLLER_NOT_FOUND", { controller: block.controller }
                );
            }

            const command = [
                "longhorn",
                "expand",
                "--size",
                `${block.size}gb`,
            ];

            const result = await this.exec(ctx, block, command);

            this.logger.info(`Expanded block controller ${block.controller} with result: ${JSON.stringify(result)}`);

            return result;
        },

        /**
         * Delete a Longhorn block controller
         * 
         * @param {Object} ctx - Molculer context
         * @param {Object} block - Block object
         * @params {Object} options - Action options
         * 
         * @returns {Promise<Object>} - Updated block controller
         */
        async deleteBlockController(ctx, block, options = {}) {

            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} not found`,
                    404, "BLOCK_CONTROLLER_NOT_FOUND", { controller: block.controller }
                );
            }

            if (block.mounted) {
                throw new MoleculerClientError(
                    `Block controller ${block.controller} is mounted`,
                    409, "BLOCK_CONTROLLER_MOUNTED", { controller: block.controller }
                );
            }

            await ctx.call("v1.kubernetes.deleteNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: block.name
            });

            const updated = await this.updateEntity(ctx, {
                id: block.id,
                controller: null,
                online: false,
            });

            this.logger.info(`Deleted block controller ${block.controller}`);

            return updated;
        },

        async updateFrontendState(ctx, block) {
            const frontendInfo = await this.getBlockControllerInfo(ctx, block)
                .catch(err => null);

            if (!frontendInfo) {
                this.logger.error(`Failed to get block controller info`);
                return;
            }

            const frontendState = frontendInfo.frontendState == "up";

            let updated = await this.updateEntity(ctx, {
                id: block.id,
                frontendState: frontendState,
                device: frontendState ? frontendInfo.endpoint : null,
                locality: block.replicas.find(r => r.healthy && r.node == block.node) ? "local" : "remote",
                healthy: block.replicas.every(r => r.healthy),
            });

            this.logger.info(`Block ${block.id} frontend state: ${frontendInfo.frontendState}`);

            if (block.frontendState !== updated.frontendState) {
                await ctx.emit(`storage.blocks.frontendState${updated.frontendState ? "Up" : "Down"}`, updated);

                if (updated.frontendState && !updated.mounted) {
                    if (!updated.formatted) {
                        updated = await this.formatBlock(ctx, updated);
                    }

                    await this.mountBlock(ctx, updated);
                } else if (!updated.frontendState && updated.mounted) {
                    await this.unmountBlock(ctx, updated);
                }
            }

            return updated;
        }
    }
};