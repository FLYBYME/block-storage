"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;
const Lock = require('../lib/lock')
const Moniker = require("moniker");
const { v4: uuid } = require('uuid');

const ConfigMixin = require("../mixins/config.mixin");

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

		/**
		 * Check block storage device pods
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * 
		 * @returns {Object} - returns the block object
		 */
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

		/**
		 * Balance block storage device
		 * 
		 * @actions
		 * @param {String} id - id of the block
		 * 
		 * @returns {Object} - returns the block object
		 */
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
		/**
		* longhorn ls command to retrieve a list of replicas for a block storage
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

		/**
		 * Update replica mode
		 * 
		 * @actions
		 * @param {String} id - block storage id
		 * @param {String} mode - replica mode
		 * 
		 * @returns {Object} - updated block storage object
		 */
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
		},
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
						const result = await this.addReplicaToFrontend(ctx, updated, replica);
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

				await this.addReplicaToFrontend(ctx, block, replica);

				await this.updateFrontendState(ctx, block);
			} else if (pod.status.phase == "Terminating" && replica.healthy) {

				await this.removeReplicaFromFrontend(ctx, block, replica)
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
				// Loop until the desired number of replicas is reached
				for (let i = replicas.length - 1; i >= desiredReplicas; i--) {
					if (replicas[i].node == block.node) {
						// Skip replicas on the same node
						continue;
					}
					// Remove the replica from the block
					updatedBlock = await this.removeReplicaFromBlock(ctx, updatedBlock, replicas[i]);
				}

				// Log the final result
				this.logger.info(`Balanced block ${block.id} down to ${desiredReplicas} replicas`);
			} else {

				// If block has locality, then move replicas to the same node
				if (block.locality == "remote") {
					// TODO: move replicas to the same node
					const disks = await ctx.call("v1.storage.disks.getNodeDisks", {
						node: block.node
					});

					if (!disks || disks.length === 0) {
						this.logger.warn(`Node ${block.node} has no storage`);
					} else {
						// Create replicas on the same disk
						updatedBlock = await this.createReplica(ctx, updated, disk);

						// Log the result
						this.logger.info(`Balanced block ${block.id} to ${desiredReplicas} replicas on node ${block.node}`);
					}
				} else {
					// Log the result
					this.logger.info(`Balanced block ${block.id} no change to ${desiredReplicas} from ${replicas.length} replicas`);
				}
			}

			// Update the frontend state if necessary
			await this.updateFrontendState(ctx, updatedBlock);

			return updatedBlock;
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
					await this.removeReplicaFromFrontend(ctx, block, replica)
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
					await this.removeReplicaFromFrontend(ctx, block, replica)
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
					await this.addReplicaToFrontend(ctx, block, replica)
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
					await this.addReplicaToFrontend(ctx, block, replica)
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
		},
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

	created() {
		this.lock = new Lock();
	},

	async started() { },

	async stopped() { }
};