const Errors = require("moleculer").Errors;
const {
    MoleculerClientError,
    MoleculerServerError,
    MoleculerRetryableError,
    MoleculerConflictDataError,
} = Errors;

module.exports = {

    actions: {
        /**
         * longhorn ls command
         * 
         * @actions
         * @param {String} id - block storage id
         * 
         * @returns {Object} - the command result
         */
        ls: {
            rest: {
                method: "GET",
                path: "/:id/ls"
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

                return this.ls(ctx, block);
            }
        },

        /**
         * longhorn info command
         * 
         * @actions
         * @param {String} id - block storage id
         * 
         * @returns {Object} - the command result
         */
        info: {
            rest: {
                method: "GET",
                path: "/:id/info"
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

                return this.info(ctx, block);
            }
        },

        /**
         * remove replica pod
         * 
         * @actions
         * @param {String} id - block storage id
         * @param {String} replica - replica pod id
         * 
         * @returns {Object} - updated block storage object
         */
        removeReplicaPod: {
            rest: {
                method: "POST",
                path: "/:id/remove-replica-pod"
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
                        `Replica with id '${ctx.params.id}' not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    );
                }

                return this.removeReplicaPod(ctx, block, replica);
            }
        },

        updateReplicaMode: {
            rest: {
                method: "POST",
                path: "/:id/update-replica-mode"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                mode: {
                    type: "string",
                    enum: [
                        "RO",
                        "RW",
                        "ERR"
                    ],
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

                return this.updateReplicaMode(ctx, block, ctx.params.mode);
            }
        },
    },

    methods: {


        /**
         * longhorn info command
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - block storage object
         * 
         * @returns {Object} - the command result
         */
        async info(ctx, block) {
            return this.exec(ctx, block, [
                "longhorn",
                "info",
            ]).then(async (res) => {
                const json = JSON.parse(res.stdout);
                return json;
            });
        },

        /**
         * longhorn ls command
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - block storage object
         * 
         * @returns {Object} - the command result
         */
        async ls(ctx, block) {
            const res = await this.exec(ctx, block, [
                "longhorn",
                "ls",
            ]);

            const lines = res.stdout.split("\n");
            const replicas = lines.slice(1)
                .filter(line => line.length > 0)
                .map(line => {
                    const parts = line.split(" ");
                    // tcp://10.42.0.18:10000 RW   [volume-head-001.img volume-snap-16e36ddf-7924-4409-994e-982f714a1c26.img]
                    const endpoint = parts.shift();
                    const mode = parts.shift();
                    const ip = endpoint.split(":")[1].replace("//", "");
                    const replica = block.replicas.find(r => r.ip === ip);

                    const volumeRegex = /\[(.*)\]/;
                    const volumeMatch = line.match(volumeRegex);
                    console.log(volumeMatch, line)
                    const volumes = volumeMatch[1].split(" ");

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
         * update replica mode
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - block storage object
         * @param {Object} replica - replica pod object
         * 
         * @returns {Object} - the block storage object
         */
        async updateReplicaMode(ctx, block, replica) {
            if (block.online) {
                // update the replica mode
                await this.exec(ctx, block, [
                    "longhorn",
                    "controller",
                    "update",
                    `tcp://${replica.ip}:10000`,
                    replica.mode
                ]);
            }

            this.logger.info(`Block storage ${block.id} replica pod ${replica.id} mode updated`);

            return block;
        },


        /**
         * info snapshots
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to list the snapshots for
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async listSnapshots(ctx, block) {
            const cmd = [
                "longhorn",
                "snapshot",
                "info"
            ];
            return this.exec(ctx, block, cmd)
                .then(res => {
                    return JSON.parse(res.stdout)
                });
        },

        /**
         * create snapshot
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to create the snapshot for
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async createSnapshot(ctx, block) {
            const cmd = [
                "longhorn",
                "snapshot",
                "create"
            ];
            return this.exec(ctx, block, cmd);
        },

        /**
         * delete snapshot
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to delete the snapshot for
         * @param {String} snapshot - the snapshot to delete
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async deleteSnapshot(ctx, block, snapshot) {
            const cmd = [
                "longhorn",
                "snapshot",
                "rm",
                snapshot
            ];
            return this.exec(ctx, block, cmd);
        },

        /**
         * revert to snapshot
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to revert to the snapshot for
         * @param {String} snapshot - the snapshot to revert to
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async revertToSnapshot(ctx, block, snapshot) {
            const cmd = [
                "longhorn",
                "snapshot",
                "revert",
                snapshot
            ];
            return this.exec(ctx, block, cmd);
        },

        /**
         * hash snapshot
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to hash the snapshot for
         * @param {String} snapshot - the snapshot to hash
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async hashSnapshot(ctx, block, snapshot) {
            const cmd = [
                "longhorn",
                "snapshot",
                "hash",
                snapshot
            ];
            return this.exec(ctx, block, cmd);
        },

        /**
         * hash snapshot status
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to hash the snapshot status for
         * @param {String} snapshot - the snapshot to hash
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async hashSnapshotStatus(ctx, block, snapshot) {
            const cmd = [
                "longhorn",
                "snapshot",
                "hash-status",
                snapshot
            ];
            return this.exec(ctx, block, cmd)
                .then(res => {
                    return JSON.parse(res.stdout);
                });
        },

        /**
         * hash snapshot cancel
         * 
         * @param {Context} ctx - context of the request
         * @param {Object} block - the block to hash the snapshot cancel for
         * @param {String} snapshot - the snapshot to hash
         * 
         * @returns {Promise<Object>} - returns the command output
         */
        async hashSnapshotCancel(ctx, block, snapshot) {
            const cmd = [
                "longhorn",
                "snapshot",
                "hash-cancel",
                snapshot
            ];
            return this.exec(ctx, block, cmd);
        },

        /**
         * execute command on block
         * 
         * @param {Context} ctx
         * @param {Object} block
         * @param {Array} cmd - command
         * 
         * @returns {Promise<string>} - command output
         */
        async exec(ctx, block, cmd) {
            console.log(cmd)
            // check if pod exists
            if (!block.controller) {
                throw new MoleculerClientError(
                    `Block storage ${block.id} has no controller pod`,
                    404, "NO_CONTROLLER", { id: block.id }
                );
            }

            const pod = await ctx.call("v1.kubernetes.readNamespacedPod", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: block.name
            });
            if (!pod) {
                throw new MoleculerClientError(
                    `Pod ${block.name} not found`,
                    404, "POD_NOT_FOUND", { id: block.name }
                );
            }

            if (pod.status.phase !== "Running") {
                throw new MoleculerClientError(
                    `Pod ${block.controller} is not running`,
                    500, "POD_NOT_RUNNING", { id: block.controller }
                );
            }

            // execute command
            return ctx.call("v1.kubernetes.exec", {
                cluster: block.cluster,
                namespace: block.namespace,
                name: block.name,
                command: cmd
            });
        },

    }

};