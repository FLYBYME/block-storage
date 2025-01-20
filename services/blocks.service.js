"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;
const Lock = require('../lib/lock')

const ConfigMixin = require("../mixins/config.mixin");
const BlockControllerMixin = require("../mixins/block.controllers.mixin");
const BlockReplicasMixin = require("../mixins/block.replicas.mixin");
const BlockSnapshotsMixin = require("../mixins/block.snapshots.mixin");
const BlockMixin = require("../mixins/block.mixin");

module.exports = {
	name: "storage.blocks",
	version: 1,

	mixins: [
		DbService({
			//createActions: false,
			adapter: {
				type: "NeDB",
				options: "./db/blocks.db"
			}
		}),
		ConfigMixin,
		BlockControllerMixin,
		BlockReplicasMixin,
		BlockMixin,
		BlockSnapshotsMixin
	],

	settings: {
		rest: '/v1/storage/blocks',
		fields: {

			// block name
			name: {
				type: "string",
				empty: false,
				required: true,
				min: 3,
				max: 128,
				trim: true,
				lowercase: true,
			},

			// block size
			size: {
				type: "number",
				empty: false,
				required: true,
				min: 1,
				max: 1024,
			},

			// block size used
			used: {
				type: "number",
				optional: true,
				required: false,
				default: 0,
			},

			// block namespace
			namespace: {
				type: "string",
				empty: false,
				required: true
			},

			// block cluster
			cluster: {
				type: "string",
				empty: false,
				required: true
			},

			// block controller pod
			controller: {
				type: "string",
				required: false
			},

			// block desired replicas
			replicaCount: {
				type: "number",
				required: false,
				default: 3,
			},

			// block replicas
			replicas: {
				type: "array",
				required: false,
				default: [],
				properties: {
					type: "object",
					props: {
						id: {
							type: "string",
							required: true,
						},
						pod: {
							type: "string",
							required: true
						},
						disk: {
							type: "string",
							required: true,
							populate: {
								action: "v1.storage.disks.resolve",
							}
						},
						node: {
							type: "string",
							required: true,
							populate: {
								action: "v1.nodes.resolve",
							}
						},
						folder: {
							type: "string",
							required: true,
							populate: {
								action: "v1.storage.folders.resolve",
							}
						},
						healthy: {
							type: "boolean",
							required: false,
							default: false,
						},
						status: {
							type: "string",
							required: false,
							default: "pending",
							enum: ["pending", "healthy", "unhealthy", "repairing", "offline"],
						},
						ip: {
							type: "string",
							required: false,
							default: null,
						},
						endpoint: {
							type: "string",
							required: false,
							default: null,
						},
						mode: {
							type: "string",
							required: false,
							default: "RW",
							enum: ["RW", "RO", "ERR"],
						},
					}
				}
			},

			// device node
			node: {
				type: "string",
				required: false,
				populate: {
					action: "v1.nodes.resolve",
				}
			},

			// block device
			device: {
				type: "string",
				required: false,
			},

			// block mount folder
			mountPoint: {
				type: "string",
				required: false,
				populate: {
					action: "v1.storage.folders.resolve",
				}
			},

			// block data locality
			locality: {
				type: "string",
				enum: [
					"local",
					"remote",
					"unknown",
				],
				required: false,
				default: "unknown",
			},

			// block formatted
			formatted: {
				type: "boolean",
				required: false,
				default: false,
			},

			// block mounted
			mounted: {
				type: "boolean",
				required: false,
				default: false,
			},

			// block online
			online: {
				type: "boolean",
				optional: true,
				required: false,
				default: false,
			},

			// block frontend state
			frontendState: {
				type: "boolean",
				optional: true,
				required: false,
				default: false,
			},

			// block healthy
			healthy: {
				type: "boolean",
				optional: true,
				required: false,
				default: false,
			},

			// block status
			status: {
				type: "string",
				optional: true,
				required: false,
				default: "pending",
				enum: ["pending", "healthy", "unhealthy", "repairing"],
			},



			id: { type: "string", primaryKey: true, columnName: "_id" },
			createdAt: {
				type: "number",
				readonly: true,
				onCreate: () => Date.now(),
				columnType: "double"
			},
			updatedAt: {
				type: "number",
				readonly: true,
				onUpdate: () => Date.now(),
				columnType: "double"
			},
			deletedAt: { type: "number", readonly: true, onRemove: () => Date.now() }
		},

		scopes: {
			notDeleted: {
				deletedAt: { $exists: false }
			},
		},

		defaultScopes: ["notDeleted"],

		config: {
			"storage.blocks.replicaCount": 3,
			"storage.blocks.replicaCountMin": 1,
			"storage.blocks.replicaCountMax": 7,

			"storage.blocks.staleReplicaTimeout": 28800,
			"storage.blocks.staleReplicaTimeoutMin": 60,
			"storage.blocks.staleReplicaTimeoutMax": 86400,

			"storage.blocks.replicaSoftAntiAffinity": true,

			"storage.blocks.engineImage": "longhornio/longhorn-engine:v1.6.0",
			"storage.blocks.replicaImage": "longhornio/longhorn-engine",

			"storage.blocks.replicaZone": "default",

			"storage.blocks.defaultSize": 10,

			"storage.blocks.defaultEngineResources.requests.memory": 64,
			"storage.blocks.defaultEngineResources.requests.cpu": 100,
			"storage.blocks.defaultEngineResources.limits.memory": 128,
			"storage.blocks.defaultEngineResources.limits.cpu": 200,

			"storage.blocks.frontend": "tgt-blockdev",

			"storage.blocks.namespace": "storage",
		},
	},

	actions: {
		/**
		 * Prrovision a new block storage
		 * 
		 * @actions
		 * @param {String} name - name of the block
		 * @param {Number} size - size of the block
		 * @param {String} node - node to create the block
		 * @param {Number} replicas - number of replicas
		 * 
		 * @returns {Object} - returns the created block object
		 */
		provision: {
			rest: {
				method: "POST",
				path: "/provision"
			},
			params: {
				name: {
					type: "string",
					empty: false,
					optional: false,
				},
				size: {
					type: "number",
					min: 1,
					max: 1024,
					optional: true,
					default: 10,
				},
				node: {
					type: "string",
					empty: false,
					optional: false,
				},
				replicas: {
					type: "number",
					min: 1,
					max: 7,
					optional: true,
					default: 3,
				},
			},
			async handler(ctx) {
				const found = await this.findEntity(ctx, {
					query: {
						name: ctx.params.name
					}
				});
				if (found) {
					throw new MoleculerClientError(
						`Block ${ctx.params.name} already exists`,
						409, "BLOCK_EXISTS", { name: ctx.params.name }
					);
				}

				const node = await ctx.call("v1.nodes.resolve", { id: ctx.params.node });
				if (!node) {
					throw new MoleculerClientError(
						`Node ${ctx.params.node} not found`,
						404, "NODE_NOT_FOUND", { node: ctx.params.node }
					);
				}

				return this.provisionBlock(ctx, ctx.params.name, node, ctx.params.size, ctx.params.replicas);
			}
		},

		/**
		 * Deprovision a block storage
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * 
		 * @returns {String} - returns the id of the deleted block
		 */
		deprovision: {
			rest: {
				method: "DELETE",
				path: "/:id/deprovision"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},

			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.deprovisionBlock(ctx, block);
			}
		},


		/**
		 * Format a block storage device
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * @param {Boolean} force - force format
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		format: {
			rest: {
				method: "POST",
				path: "/:id/format"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
				force: {
					type: "boolean",
					optional: true,
					default: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.formatBlock(ctx, block, { force: ctx.params.force });
			}
		},

		/**
		 * Mount a block storage device
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * @param {Boolean} force - force mount
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		mount: {
			rest: {
				method: "POST",
				path: "/:id/mount"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
				force: {
					type: "boolean",
					optional: true,
					default: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.mountBlock(ctx, block, { force: ctx.params.force });
			}
		},

		/**
		 * Unmount a block storage device
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * @param {Boolean} force - force unmount
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		unmount: {
			rest: {
				method: "POST",
				path: "/:id/unmount"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
				force: {
					type: "boolean",
					optional: true,
					default: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.unmountBlock(ctx, block, { force: ctx.params.force });
			}
		},

		/**
		 * Get block storage device usage
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * 
		 * @returns {Object} - returns the block usage
		 */
		usage: {
			rest: {
				method: "GET",
				path: "/:id/usage"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.getUsage(ctx, block);
			}
		},

		/**
		 * Trim block storage device
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * 
		 * @returns {Object} - returns the block object
		 */
		trim: {
			rest: {
				method: "POST",
				path: "/:id/trim"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.trimBlock(ctx, block);
			}
		},

		checkPods: {
			rest: {
				method: "GET",
				path: "/:id/check-pods"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.checkPods(ctx, block);
			}
		},

		balanceBlock: {
			rest: {
				method: "POST",
				path: "/:id/balance"
			},
			params: {
				id: {
					type: "string",
					empty: false,
					optional: false,
				},
			},
			async handler(ctx) {
				const block = await this.findById(ctx, ctx.params.id);

				if (!block) {
					throw new MoleculerClientError(
						`Block ${ctx.params.id} not found`,
						404, "BLOCK_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.balanceBlock(ctx, block);
			}
		},
	},

	events: {
		async "kubernetes.pods.deleted"(ctx) {
			const pod = ctx.params;
		},
		async "kubernetes.pods.added"(ctx) {
			const pod = ctx.params;
		},
		
		async "kubernetes.pods.modified"(ctx) {
			const pod = ctx.params;

			if (pod.metadata.namespace !== this.config.get("storage.blocks.namespace")) {
				return;
			}
			await this.lock.acquire("blocks");

			const block = await this.findByPod(ctx, pod.metadata.uid);

			if (!block) {
				await this.lock.release("blocks");
				return;
			}

			if (pod.metadata.uid == block.controller) {
				if (pod.status.phase == "Running" && !block.online) {
					const updated = await this.updateEntity(ctx, {
						id: block.id,
						online: true,
					});

					this.logger.info(`Block ${block.id} is online`);

					// add all replicas to the block
					for (let replica of updated.replicas) {
						const result = await this.addReplica(ctx, updated, replica);
						this.logger.info(`Replica ${replica.id} added to block ${block.id}`);
					}

					await this.updateFrontendState(ctx, updated);

					await this.lock.release("blocks");

					return;
				} else if (pod.status.phase == "Terminating" && block.online) {
					const updated = await this.updateEntity(ctx, {
						id: block.id,
						online: false,
						mounted: false,
						frontendState: false,
						device: null
					});

					this.logger.info(`Block ${block.id} is offline`);

					await this.lock.release("blocks");

					return;
				}
			}

			const replica = block.replicas.find(r => r.pod === pod.metadata.uid);
			if (!replica) {
				await this.lock.release("blocks");
				return;
			}

			if (pod.status.phase == "Running" && !replica.healthy) {

				replica.healthy = true;
				replica.status = "healthy";
				replica.ip = pod.status.podIP;
				replica.endpoint = `tcp://${pod.status.podIP}:10000`;

				await this.updateEntity(ctx, {
					id: block.id,
					replicas: block.replicas
				});

				this.logger.info(`Replica ${replica.id} is healthy`);

				await this.addReplica(ctx, block, replica);

				await this.updateFrontendState(ctx, block);
			} else if (pod.status.phase == "Terminating" && replica.healthy) {

				await this.removeReplica(ctx, block, replica)
					.catch((err) => {
						this.logger.error(`Failed to remove replica ${replica.id} from block ${block.id}: ${err.message}`);
					});

				replica.pod = null;
				replica.healthy = false;
				replica.status = "unhealthy";
				replica.ip = null;
				replica.endpoint = null;

				await this.updateEntity(ctx, {
					id: block.id,
					replicas: block.replicas
				});

				this.logger.info(`Replica ${replica.id} is unhealthy`);

				await this.updateFrontendState(ctx, block);
			}
			await this.lock.release("blocks");
		}
	},

	methods: {
		/**
		 * find block by id
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {String} id - id of the block
		 * 
		 * @returns {Promise<Object>} - returns the block
		 */
		async findById(ctx, id) {
			return this.resolveEntities(ctx, { id });
		},

		/**
		 * get the block by pod id
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {String} podID - id of the pod
		 * 
		 * @returns {Object} - the block object
		 */
		async findByPod(ctx, podID) {
			const query = {
				$or: [
					{ controller: podID },
					{ 'replicas.pod': podID }
				]
			};

			return this.findEntity(ctx, {
				query
			});
		},

		async checkPods(ctx, block) {

			// check controller
			await this.checkControllerPods(ctx, block);

			// check replicas
			await this.checkReplicaPods(ctx, block);

			// update frontend state
			return this.updateFrontendState(ctx, block);
		},


		/**
		 * Balance the number of replicas of a block storage to the desired count
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block storage object
		 * 
		 * @returns {Promise<Object>} - returns the updated block object
		 */
		async balanceBlock(ctx, block) {
			// Get the current number of replicas of the block
			const replicas = block.replicas || [];

			// Get the desired number of replicas
			const desiredReplicas = block.replicaCount || 3;

			// Initialize the updated block object
			let updatedBlock = block;

			// If the current number of replicas is less than the desired number, add replicas
			if (replicas.length < desiredReplicas) {
				// Loop until the desired number of replicas is reached
				for (let i = replicas.length; i < desiredReplicas; i++) {
					// Find an available disk
					const disk = await ctx.call("v1.storage.disks.availableDisks", {
						cluster: block.cluster,
						size: block.size * 1024,
						exclude: updatedBlock.replicas.map(r => r.disk),
						limit: 1
					}).then(r => r[0]);

					// Ensure the disk is available
					if (!disk) {
						this.logger.warn(`No available disk found for block ${block.id}`);
						return updatedBlock;
					}

					// Add the replica to the block
					updatedBlock = await this.createReplica(ctx, updatedBlock, disk);

					// Log the result
					this.logger.info(`Added replica ${updatedBlock.replicas[updatedBlock.replicas.length - 1].id} to block ${block.id}`);
				}

				// Log the final result
				this.logger.info(`Balanced block ${block.id} up to ${desiredReplicas} replicas`);
			} else if (replicas.length > desiredReplicas) {
				// TODO: first remove replicas that share the same disk. then remove most constrained node
			} else {
				// Log the result if no change
				this.logger.info(`Balanced block ${block.id} no change to ${desiredReplicas} from ${replicas.length} replicas`);
			}

			// Update the frontend state if necessary
			await this.updateFrontendState(ctx, updatedBlock);
		},

		/**
		 * Check the state of the replicas of a block storage
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block storage object
		 * 
		 * @returns {Promise<Object>} - returns the updated block object
		 */
		async checkReplicaPods(ctx, block) {
			// Loop through all replicas of the block
			for (let replica of block.replicas) {
				// Get the pod object of the replica
				const pod = await ctx.call("v1.kubernetes.readNamespacedPod", {
					cluster: block.cluster,
					namespace: block.namespace,
					name: replica.name
				});

				// If the pod is not found
				if (!pod) {
					// Remove the replica from the block
					await this.removeReplica(ctx, block, replica)
						.catch((err) => {
							this.logger.error(`Failed to remove replica ${replica.id} from block ${block.id}: ${err.message}`);
						});

					// Set the replica as unhealthy
					replica.healthy = false;
					replica.status = "unhealthy";
					replica.ip = null;
					replica.endpoint = null;
					replica.attached = false;

					// Update the block object
					await this.updateEntity(ctx, {
						id: block.id,
						replicas: block.replicas
					});

					// Log the result
					this.logger.warn(`Replica ${replica.id} is unhealthy, not found`);
				} else if (pod.status.phase != "Running") {
					// If the pod is not running
					// Remove the replica from the block
					await this.removeReplica(ctx, block, replica)
						.catch((err) => {
							this.logger.error(`Failed to remove replica ${replica.id} from block ${block.id}: ${err.message}`);
						});

					// Set the replica as unhealthy
					replica.pod = null;
					replica.healthy = false;
					replica.status = "unhealthy";
					replica.ip = null;
					replica.endpoint = null;
					replica.attached = false;

					// Update the block object
					await this.updateEntity(ctx, {
						id: block.id,
						replicas: block.replicas
					});

					// Log the result
					this.logger.warn(`Replica ${replica.id} is unhealthy`);

					// Try to add the replica back to the block
					await this.addReplica(ctx, block, replica)
						.catch((err) => {
							this.logger.error(`Failed to add replica ${replica.id} to block ${block.id}: ${err.message}`);
						});
				} else if (pod.status.phase == "Running" && !replica.healthy) {
					// If the pod is running and the replica is not healthy
					// Set the replica as healthy
					replica.healthy = true;
					replica.status = "healthy";
					replica.ip = pod.status.podIP;
					replica.endpoint = `tcp://${pod.status.podIP}:10000`;

					// Update the block object
					await this.updateEntity(ctx, {
						id: block.id,
						replicas: block.replicas
					});

					// Log the result
					this.logger.info(`Replica ${replica.id} is healthy`);

					// Try to add the replica back to the block
					await this.addReplica(ctx, block, replica)
						.catch((err) => {
							this.logger.error(`Failed to add replica ${replica.id} to block ${block.id}: ${err.message}`);
						});
				}
			}
		},

		/**
		 * Check the state of the controller pod of a block storage
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block storage object
		 * 
		 * @returns {Promise<Object>} - returns the updated block object
		 */
		async checkControllerPods(ctx, block) {
			// Check if the controller pod is running
			const controller = await ctx.call("v1.kubernetes.readNamespacedPod", {
				cluster: block.cluster,
				namespace: block.namespace,
				name: block.name
			});

			// If the controller pod is not found
			if (!controller) {
				throw new MoleculerClientError(
					`Controller pod ${block.controller} not found`,
					404, "CONTROLLER_NOT_FOUND", { controller: block.controller }
				);
			}

			// If the controller pod is not running
			if (controller.status.phase != "Running") {
				// Set the block as offline
				return this.updateEntity(ctx, {
					id: block.id,
					online: false,
					mounted: false,
					frontendState: false,
					device: null
				});
			}

			// If the controller pod is running and the block is not online
			if (controller.status.phase == "Running" && !block.online) {
				// Set the block as online
				return this.updateEntity(ctx, {
					id: block.id,
					online: true,
				});
			}

			// If the controller pod is running and the block is already online
			// Just return the block object
			return block;
		},

		/**
		 * Provision a new block storage
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {String} name - name of the block
		 * @param {Object} node - node object
		 * @param {Number} size - size of the block in GB
		 * @param {Number} replicas - number of replicas to create
		 * 
		 * @returns {Object} - returns the created block object
		 */
		async provisionBlock(ctx, name, node, size, replicas) {
			// Retrieve all disks associated with the given node
			const disks = await ctx.call("v1.storage.disks.getNodeDisks", {
				node: node.id
			});

			// If no disks are found, throw an error
			if (!disks || disks.length === 0) {
				throw new MoleculerClientError(
					`Node ${node.id} has no storage`,
					404, "NODE_STORAGE_NOT_FOUND", { node: node.id }
				);
			}

			// Provision a storage folder on the first available disk
			const mountPoint = await ctx.call("v1.storage.folders.provision", {
				prefix: "block",
				disk: disks[0].id,
			});

			// Create a new block entity with the specified parameters
			const block = await this.createEntity(ctx, {
				name,
				size,
				node: node.id,
				replicaCount: replicas,
				cluster: this.config.get("storage.defaultCluster"),
				namespace: this.config.get("storage.blocks.namespace"),
				mountPoint: mountPoint.id,
				replicas: [], // Initialize with an empty list of replicas
			});

			// Log the creation of the block
			this.logger.info(`Block ${block.id} created`);

			// Create a controller for the block
			await this.createBlockController(ctx, block);

			// Retrieve a list of available disks for creating replicas
			const availableDisks = await ctx.call("v1.storage.disks.availableDisks", {
				cluster: block.cluster,
				size: block.size * 1024, // Convert size to MB
				limit: replicas // Limit the number of disks to the number of replicas needed
			});

			let updated = block;

			// Iterate over the list of available disks and create replicas
			for (let disk of availableDisks) {
				updated = await this.createReplica(ctx, updated, disk);
				// Log the creation of each replica
				this.logger.info(`Replica created for block ${block.id}`);
			}

			// Check if the number of created replicas is less than desired
			if (updated.replicas.length < replicas) {
				// Log a warning if not enough replicas were created
				this.logger.warn(`Not enough replicas created for block ${block.id}`);
			}

			// Return the updated block object
			return updated;
		},

		/**
		 * Deprovision a block
		 * 
		 * This action will:
		 *  - Check if the block is mounted and throw an error if it is
		 *  - Delete the Longhorn controller pod for the block
		 *  - Remove each replica from the block
		 *  - Deprovision the storage folder associated with the block
		 *  - Remove the block entity from the database
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * 
		 * @returns {Object} - returns the deleted block object
		 */
		async deprovisionBlock(ctx, block) {
			// Check if the block is mounted
			if (block.mounted) {
				throw new MoleculerClientError(
					`Block ${block.id} is mounted`,
					409, "BLOCK_MOUNTED", { id: block.id }
				);
			}

			// Delete the Longhorn controller pod for the block
			// This will also remove the block controller object from the database
			await this.deleteBlockController(ctx, block)
				.catch(err => {
					this.logger.error(`Error deleting controller for block ${block.id}`, err);
				});

			// Remove each replica from the block
			// This will also remove the replica objects from the database
			for (let replica of block.replicas) {
				await this.removeReplicaFromBlock(ctx, block, replica)
					.catch(err => {
						this.logger.error(`Error removing replica ${replica.id} from block ${block.id}`, err);
					});
			}

			// Deprovision the storage folder associated with the block
			await ctx.call("v1.storage.folders.deprovision", { id: block.mountPoint });

			// Remove the block entity from the database
			return this.removeEntity(ctx, {
				id: block.id
			});
		},

		/**
		 * Format a block storage device
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * @param {Object} options - format options
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		async formatBlock(ctx, block, options = {}) {

			if (block.formatted && !options.force) {
				throw new MoleculerClientError(
					`Block ${block.id} is formatted`,
					409, "BLOCK_FORMATED", { id: block.id }
				);
			}

			if (block.mounted) {
				throw new MoleculerClientError(
					`Block ${block.id} is mounted`,
					409, "BLOCK_MOUNTED", { id: block.id }
				);
			}

			const command = [
				"mkfs",
				"-t",
				options.type || "ext4",
				"-m",
				options.reserve || 0,
				"-L",
				block.name,
				block.device,
			];

			const result = await this.exec(ctx, block, command);


			const updated = await this.updateEntity(ctx, {
				id: block.id,
				formatted: true,
			});

			this.logger.info(`Block ${block.id} formatted`);

			return updated;
		},

		/**
		 * Mount a block storage device
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * @param {Object} options - mount options
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		async mountBlock(ctx, block, options = {}) {

			if (block.mounted && !options.force) {
				throw new MoleculerClientError(
					`Block ${block.id} is mounted`,
					409, "BLOCK_MOUNTED", { id: block.id }
				);
			}

			if (!block.formatted) {
				throw new MoleculerClientError(
					`Block ${block.id} is not formatted`,
					409, "BLOCK_NOT_FORMATED", { id: block.id }
				);
			}

			const mountPoint = await ctx.call("v1.storage.folders.resolve", { id: block.mountPoint });

			const command = [
				"mount",
				block.device,
				mountPoint.path,
			];

			const result = await this.exec(ctx, block, command);

			const updated = await this.updateEntity(ctx, {
				id: block.id,
				mounted: true,
			});

			this.logger.info(`Block ${block.id} mounted`);

			return updated;
		},

		/**
		 * Unmount a block storage device
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * @param {Object} options - unmount options
		 * 
		 * @returns {Object} - returns the updated block object
		 */
		async unmountBlock(ctx, block, options = {}) {
			if (!block.mounted && !options.force) {
				throw new MoleculerClientError(
					`Block ${block.id} is not mounted`,
					409, "BLOCK_NOT_MOUNTED", { id: block.id }
				);
			}

			const mountPoint = await ctx.call("v1.storage.folders.resolve", { id: block.mountPoint });

			const command = [
				"umount",
				mountPoint.path,
			];

			const result = await this.exec(ctx, block, command);

			const updated = await this.updateEntity(ctx, {
				id: block.id,
				mounted: false,
			});

			this.logger.info(`Block ${block.id} unmounted`);

			return updated;
		},

		/**
		 * get block storage device usage
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * 
		 * @returns {Object} - returns the block usage
		 */
		async getUsage(ctx, block) {
			const mountPoint = await ctx.call("v1.storage.folders.resolve", { id: block.mountPoint });

			const result = await this.exec(ctx, block, ["df", mountPoint.path]);

			const split = result.stdout.split("\n");

			const usage = split[1].split(/\s+/);
			const size = Number(usage[1]) / 1024 / 1024;
			const used = Number(usage[2]) / 1024 / 1024;
			const available = Number(usage[3]) / 1024 / 1024;
			const usedPercent = Number(usage[4].replace("%", ""));

			await this.updateEntity(ctx, {
				id: block.id,
				used,
			});

			return {
				size,
				used,
				available,
				usedPercent,
				replicas: await this.getReplicaUsage(ctx, block),
			};
		},

		/**
		 * get replica folder usage
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * 
		 * @returns {Object} - returns the replica usage
		 */
		async getReplicaUsage(ctx, block) {

			const replicas = block.replicas;

			const usage = [];

			for (let replica of replicas) {

				if (replica.status !== "healthy") {
					// replica is not healthy
					usage.push({
						name: replica.name,
						id: replica.id,
						used: -1
					})
					this.logger.info(`Block storage ${block.id} replica ${replica.id} is not healthy`);
					continue;
				}

				const folder = await ctx.call("v1.storage.folders.resolve", { id: replica.folder });

				const result = await this.exec(ctx, block, ["du", "-s", folder.path]);

				const line = result.stdout.split("\n")[0];
				const value = line.split(/\s+/);
				const size = Number(value[0]) / 1024 / 1024;

				usage.push({
					name: replica.name,
					id: replica.id,
					used: size
				});
			}

			return usage;
		},

		/**
		 * trim block storage device
		 * 
		 * @param {Context} ctx - context of the request
		 * @param {Object} block - block object
		 * 
		 * @returns {Object} - returns the block object
		 */
		async trim(ctx, block) {
			const mountPoint = await ctx.call("v1.storage.folders.resolve", { id: block.mountPoint });

			const result = await this.exec(ctx, block, ["fstrim", mountPoint.path]);

			this.logger.info(`Block ${block.id} trimmed`);

			return block;
		},
	},

	created() {
		this.lock = new Lock();
	},

	async started() { },

	async stopped() { }
};