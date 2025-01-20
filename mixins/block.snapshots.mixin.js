// NAME:
//    longhorn snapshots -
// USAGE:
//    longhorn snapshots command [command options] [arguments...]
// COMMANDS:
//    create
//    revert
//    ls
//    rm
//    purge
//    purge-status
//    info
//    clone
//    clone-status
//    hash
//    hash-cancel
//    hash-status
// OPTIONS:
//    --help, -h  show help

const moleculer = require('moleculer');
const MoleculerClientError = moleculer.Errors.MoleculerClientError;

// Define the mixin
module.exports = {
    name: "k8s.storage.blocks.snapshots",
    settings: {
        // Define the mixin specific settings
    },
    actions: {
        // Define the mixin specific actions

        /**
         * Create a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * 
         * @returns {Object} - Block object
         */
        createSnapshot: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/create"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.createSnapshot(ctx, block);
            }
        },

        /**
         * Revert a volume to a specific snapshot
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        revertSnapshot: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/revert"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.revertSnapshot(ctx, block, ctx.params.snapshotName);
            }
        },

        /**
         * List snapshots of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * 
         * @returns {Array} - Array of snapshot objects
         */
        listSnapshots: {
            rest: {
                method: "GET",
                path: "/:id/snapshots/list"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.listSnapshots(ctx, block);
            }
        },

        /**
         * Remove a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        removeSnapshot: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/remove"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.removeSnapshot(ctx, block, ctx.params.snapshotName);
            }
        },

        /**
         * Purge snapshots of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {Boolean} skipInProgress - set to mute errors if replica is already purging
         * 
         * @returns {Object} - Block object
         */
        purgeSnapshots: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/purge"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                skipInProgress: {
                    type: "boolean",
                    default: false,
                    optional: true
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.purgeSnapshots(ctx, block, ctx.params.skipInProgress);
            }
        },

        /**
         * Get status of the purge operation of snapshots of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * 
         * @returns {Object} - Block object
         */
        purgeSnapshotsStatus: {
            rest: {
                method: "GET",
                path: "/:id/snapshots/purge-status"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.purgeSnapshotsStatus(ctx, block);
            }
        },

        /**
         * Get information about a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Snapshot object
         */
        getSnapshotInfo: {
            rest: {
                method: "GET",
                path: "/:id/snapshots/info"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.getSnapshotInfo(ctx, block);
            }
        },

        /**
         * Clone a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshot-name - Specify the name of snapshot needed to clone
         * @param {String} from-controller-address - Specify the address of the engine controller of the source volume
         * @param {String} from-volume-name - Specify the name of the source volume (for validation purposes)
         * @param {String} from-controller-instance-name - Specify the name of the engine controller instance of the source volume (for validation purposes)
         * 
         * @returns {Object} - Block object
         */
        cloneSnapshot: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/clone"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                fromControllerAddress: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                fromVolumeName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                fromControllerInstanceName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.cloneSnapshot(ctx, block, ctx.params.snapshotName,
                    ctx.params.fromControllerAddress, ctx.params.fromVolumeName, ctx.params.fromControllerInstanceName);
            }
        },

        /**
         * Get status of the clone operation of a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        cloneSnapshotStatus: {
            rest: {
                method: "GET",
                path: "/:id/snapshots/clone-status"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.cloneSnapshotStatus(ctx, block, ctx.params.snapshotName);
            }
        },

        /**
         * Get hash of a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        startSnapshotHash: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/hash"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.startSnapshotHash(ctx, block, ctx.params.snapshotName);
            }
        },

        /**
         * Cancel the hash calculation of a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        cancelSnapshotHash: {
            rest: {
                method: "POST",
                path: "/:id/snapshots/hash-cancel"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.cancelSnapshotHash(ctx, block, ctx.params.snapshotName);
            }
        },
        /**
         * Get status of the hash calculation of a snapshot of a volume
         * 
         * @actions
         * @param {String} id - ID of the block
         * @param {String} snapshotName - Name of the snapshot
         * 
         * @returns {Object} - Block object
         */
        getSnapshotHashStatus: {
            rest: {
                method: "GET",
                path: "/:id/snapshots/hash-status"
            },
            params: {
                id: {
                    type: "string",
                    empty: false,
                    optional: false
                },
                snapshotName: {
                    type: "string",
                    empty: false,
                    optional: false
                },
            },
            async handler(ctx) {
                const block = await this.findById(ctx, ctx.params.id);
                if (!block) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} not found`,
                        404, "NOT_FOUND", { id: ctx.params.id }
                    )
                }

                if (!block.online) {
                    return MoleculerClientError(
                        `Block with ID ${ctx.params.id} is not online`,
                        400, "BAD_REQUEST", { id: ctx.params.id }
                    )
                }

                return this.getSnapshotHashStatus(ctx, block, ctx.params.snapshotName);
            }
        },


    },

    // Define the mixin specific methods
    methods: {
        // Define the mixin specific methods

        /**
         * Create a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Object>} - Block object
         */
        async createSnapshot(ctx, block) {
            const command = [
                "longhorn",
                "snapshots",
                "create"
            ];

            const result = await this.exec(ctx, block, command);

            // TODO: create new entity of the snapshot
            return result;
        },

        /**
         * Revert a volume to a specific snapshot
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async revertSnapshot(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "revert",
                snapshotName,
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * List snapshots of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Array>} - Array of snapshot IDs
         */
        async listSnapshots(ctx, block) {
            const command = [
                "longhorn",
                "snapshots",
                "ls"
            ];

            const result = await this.exec(ctx, block, command);

            const lines = result.stdout.split("\n");

            lines.shift(); // remove header line

            return lines.filter(line => line.length > 0);
        },

        /**
         * Remove a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async removeSnapshot(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "rm",
                snapshotName
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Purge snapshots of a volume
         * 
         * When the snapshot purge is triggered, replicas will identify if the snapshot being removed 
         * is the latest snapshot by checking one child of it is the volume head. If YES, they will
         * start the snapshot pruning operation:
         * 
         * Before pruning, replicas will make sure the apparent size of the snapshot is the same as that of the volume head. 
         * If No, we will truncate/expand the snapshot first.
         * 
         * During pruning, replicas need to iterate the volume head fiemap. Then as long as there is a data chunk found
         * in the volume head file, they will blindly punch a hole at the same position of the snapshot file. 
         * If there are multiple snapshots including the latest one being removed simultaneously, 
         * we need to make sure the pruning is done only after all the other snapshots have done coalescing and deletion.
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {Boolean} skipInProgress - set to mute errors if replica is already purging
         * 
         * @returns {Promise<Object>} - Block object
         */
        async purgeSnapshots(ctx, block, skipInProgress) {
            const command = [
                "longhorn",
                "snapshots",
                "purge"
            ];

            if (skipInProgress) {
                command.push("--skip-if-in-progress");
            }

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Get status of the purge operation of snapshots of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async purgeSnapshotsStatus(ctx, block, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "purge-status"
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Get information about a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * 
         * @returns {Promise<Object>} - Snapshot object
         */
        async getSnapshotInfo(ctx, block) {
            const command = [
                "longhorn",
                "snapshots",
                "info"
            ];

            const result = await this.exec(ctx, block, command);

            return JSON.parse(result.stdout);
        },

        /**
         * Clone a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshot-name - Specify the name of snapshot needed to clone
         * @param {String} from-controller-address - Specify the address of the engine controller of the source volume
         * @param {String} from-volume-name - Specify the name of the source volume (for validation purposes)
         * @param {String} from-controller-instance-name - Specify the name of the engine controller instance of the source volume (for validation purposes)
         * 
         * @returns {Promise<Object>} - Block object
         */
        async cloneSnapshot(ctx, block, snapshotName, fromControllerAddress, fromVolumeName, fromControllerInstanceName) {
            const command = [
                "longhorn",
                "snapshots",
                "clone",
                "--snapshot-name",
                snapshotName,
                "--from-controller-address",
                fromControllerAddress,
                "--from-volume-name",
                fromVolumeName,
                "--from-controller-instance-name",
                fromControllerInstanceName
            ];

            const result = await this.exec(ctx, block, command);

            return result;

        },

        /**
         * Get status of the clone operation of a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async cloneSnapshotStatus(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "clone-status",
                snapshotName
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Get hash of a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async startSnapshotHash(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "hash",
                snapshotName
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Cancel the hash calculation of a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async cancelSnapshotHash(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "hash-cancel",
                snapshotName
            ];

            const result = await this.exec(ctx, block, command);

            return result;
        },

        /**
         * Get status of the hash calculation of a snapshot of a volume
         * 
         * @param {Context} ctx - Moleculer Context instance of the current action
         * @param {Object} block - Block object
         * @param {String} snapshotName - Name of the snapshot
         * @param {String} volumeName - Name of the volume
         * 
         * @returns {Promise<Object>} - Block object
         */
        async getSnapshotHashStatus(ctx, block, snapshotName, volumeName) {
            const command = [
                "longhorn",
                "snapshots",
                "hash-status",
                snapshotName
            ];

            const result = await this.exec(ctx, block, command);

            return JSON.parse(result.stdout);
        },


    }
};