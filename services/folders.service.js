"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;
const crypto = require("crypto");
const path = require("path");

const ConfigMixin = require("../mixins/config.mixin");


module.exports = {
	name: "storage.folders",
	version: 1,

	mixins: [
		DbService({
			adapter: {
				type: "NeDB",
				options: "./db/storage.folders.db"
			}
		}),
		ConfigMixin
	],

	settings: {
		fields: {
			// folder name
			name: {
				type: "string",
				required: true,
				empty: false,
				unique: true
			},

			// disk id
			disk: {
				type: "string",
				required: true,
				empty: false,
				populate: {
					action: "v1.storage.disks.resolve",
				}
			},

			// node id
			node: {
				type: "string",
				required: true,
				empty: false,
				populate: {
					action: "v1.nodes.resolve",
				}
			},

			// cluster id
			cluster: {
				type: "string",
				required: true,
				empty: false,
			},

			// folder path
			path: {
				type: "string",
				required: true,
				empty: false,
			},

			// nfs
			nfs: {
				type: "object",
				props: {
					server: {
						type: "string",
						required: true,
						empty: false,
					},
					path: {
						type: "string",
						required: true,
						empty: false,
					}
				}
			},

			// folder used
			used: {
				type: "number",
				required: false,
				default: 0
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

		}
	},

	actions: {

		/**
		 * Provision a new folder
		 * 
		 * @actions
		 * @param {String} prefix - prefix of the folder name
		 * @param {String} disk - disk id
		 * 
		 * @returns {Object} folder
		 */
		provision: {
			rest: {
				method: "POST",
				path: "/provision",
			},
			params: {
				prefix: { type: "string", optional: true, empty: false, default: "folder" },
				disk: { type: "string", optional: false },
			},
			async handler(ctx) {
				const disk = await ctx.call("v1.storage.disks.resolve", { id: ctx.params.disk });
				if (!disk) {
					throw new MoleculerClientError(
						`Disk with id ${ctx.params.disk} not found`,
						404, "DISK_NOT_FOUND", { id: ctx.params.disk }
					);
				}

				return this.provisionFolder(ctx, ctx.params.prefix, disk);
			}
		},

		/**
		 * Deprovision a folder
		 * 
		 * @actions
		 * @param {String} id - folder id
		 * 
		 * @returns {Object} folder
		 */
		deprovision: {
			rest: {
				method: "DELETE",
				path: "/:id/deprovision",
			},
			params: {
				id: { type: "string", optional: false },
			},
			async handler(ctx) {
				const folder = await ctx.call("v1.storage.folders.resolve", { id: ctx.params.id });
				if (!folder) {
					throw new MoleculerClientError(
						`Folder with id ${ctx.params.id} not found`,
						404, "FOLDER_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.deprovisionFolder(ctx, folder);
			}
		},

		/**
		 * Folder used size
		 * 
		 * @actions
		 * @param {String} id - folder id
		 * 
		 * @returns {Number} size
		 */
		updateUsed: {
			rest: {
				method: "GET",
				path: "/:id/used"
			},
			params: {
				id: { type: "string", empty: false, optional: false }
			},
			async handler(ctx) {
				const params = ctx.params;

				const folder = await this.findByID(ctx, params.id);
				if (!folder) {
					throw new MoleculerClientError(
						`Folder with id ${params.id} not found`,
						404, "FOLDER_NOT_FOUND", { id: params.id }
					);
				}

				const used = await this.getUsed(ctx, folder);

				return this.updateEntity(ctx, { id: folder.id, used: used });
			}
		},

		/**
		 * Cron job to update folder used size
		 * 
		 * @actions
		 * 
		 * @returns {Number} size
		 */
		updateAllUsed: {
			rest: {
				method: "GET",
				path: "/update/used"
			},
			async handler(ctx) {
				const folders = await this.findEntities(ctx, {});

				for (let folder of folders) {
					await this.actions.updateUsed(ctx, { id: folder.id })
						.catch(err => {
							this.logger.error(`Error updating folder ${folder.id} used size`, err);
						});
				}

				return folders;
			}
		},
	},

	events: {

	},

	methods: {

		/**
		 * find by id
		 * 
		 * @param {Context} ctx - moleculer context
		 * @param {String} id - folder id
		 * 
		 * @returns {Object} folder
		 */
		async findByID(ctx, id) {
			return this.resolveEntities(ctx, {
				id: id
			});
		},

		/**
		 * provision a new folder on disk
		 * 
		 * @param {Context} ctx - moleculer context
		 * @param {String} prefix - prefix of the folder name
		 * @param {Object} disk - disk entity
		 * 
		 * @returns {Object} folder
		 */
		async provisionFolder(ctx, prefix, disk) {

			const folderName = `${prefix}-${crypto.randomBytes(4).toString('hex')}`;
			const diskMountPoint = await ctx.call("v1.storage.disks.mountPoint", { id: disk.id });
			const folderPath = `${diskMountPoint}/${folderName}`;

			await ctx.call("v1.storage.disks.createFolder", {
				id: disk.id,
				path: folderName
			});

			const nfs = await ctx.call("v1.storage.disks.nfs", {
				id: disk.id,
				path: folderName
			});

			const folder = await this.createEntity(ctx, {
				name: folderName,
				disk: disk.id,
				node: disk.node,
				cluster: disk.cluster,
				path: folderPath,
				nfs: {
					server: nfs.server,
					path: nfs.path
				}
			});

			return folder;
		},

		/**
		 * deprovision a folder
		 * 
		 * @param {Context} ctx - moleculer context
		 * @param {Object} folder - folder entity
		 * 
		 * @returns {Object} folder
		 */
		async deprovisionFolder(ctx, folder) {

			const disk = await ctx.call("v1.storage.disks.resolve", { id: folder.disk });
			if (!disk) {
				throw new MoleculerClientError(
					`Disk with id ${folder.disk} not found`,
					404, "DISK_NOT_FOUND", { id: folder.disk }
				);
			}

			const diskMountPoint = await ctx.call("v1.storage.disks.mountPoint", { id: disk.id });
			const folderPath = `${diskMountPoint}/${folder.name}`;

			await ctx.call("v1.storage.disks.removeFolder", {
				id: folder.disk,
				path: folder.name
			});

			await this.removeEntity(ctx, { id: folder.id });

			return folder;

		},

		/**
		 * get folder used size
		 * 
		 * @param {Context} ctx - moleculer context
		 * @param {Object} folder - folder entity
		 * 
		 * @returns {Number} size
		 */
		async getUsed(ctx, folder) {
			const used = await ctx.call("v1.storage.disks.getFolderSize", {
				id: folder.disk,
				path: folder.path
			});

			return used;
		},
	},

	created() { },

	async started() { },

	async stopped() { }
};