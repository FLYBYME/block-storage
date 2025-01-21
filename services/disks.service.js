"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;
const path = require("path");

const ConfigMixin = require("../mixins/config.mixin");


module.exports = {
	name: "storage.disks",
	version: 1,

	mixins: [
		DbService({
			createActions: true,
			adapter: {
				type: "NeDB",
				options: "./db/storage.disks.db"
			}
		}),
		ConfigMixin
	],

	settings: {
		fields: {

			node: {
				type: "string",
				required: true,
				empty: false,
				populate: {
					action: "v1.nodes.resolve",
				},
			},

			name: {
				type: "string",
				required: true,
				empty: false,
			},
			size: {
				type: "number",
				required: true,
			},

			used: {
				type: "number",
				required: false,
			},
			free: {
				type: "number",
				required: false,
			},

			rota: {
				type: "boolean",
				required: true,
			},
			ro: {
				type: "boolean",
				required: true,
			},
			model: {
				type: "string",
				required: false,
				empty: false,
			},
			serial: {
				type: "string",
				required: false,
				empty: false,
			},

			mountpoint: {
				type: "string",
				required: false,
				empty: true,
			},

			nfs: {
				type: "string",
				empty: false,
				required: false,
				populate: {
					action: "v1.storage.nfs.resolve"
				}
			},

			cluster: {
				type: "string",
				empty: false,
				required: false
			},

			fsType: {
				type: "string",
				empty: false,
				required: false
			},
			formatted: {
				type: "boolean",
				required: false
			},
			mounted: {
				type: "boolean",
				required: false
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
			"storage.disks.defaultCluster": "test",
			"storage.disks.namespace": "storage",
			"storage.disks.fsType": "ext4",
		}
	},

	actions: {
		/**
		 * Get available disks
		 * 
		 * @actions
		 * @param {String} cluster - cluster name
		 * @param {Number} size - size in MB that should be available on the disk
		 * @param {Number} limit - limit number of disks to return
		 * @param {Array} exclude - array of disk ids to exclude
		 * 
		 * @returns {Object} storage devices
		 */
		availableDisks: {
			rest: {
				method: "POST",
				path: "/available",
			},
			params: {
				cluster: { type: "string", empty: false, required: true },
				size: { type: "number", empty: false, required: true },
				limit: { type: "number", empty: false, required: false, default: 1 },
				exclude: { type: "array", empty: false, required: false },
			},
			async handler(ctx) {
				const disks = await ctx.call("v1.storage.disks.find", {
					query: {
						cluster: ctx.params.cluster,
						nfs: { $exists: true },
						free: { $gte: ctx.params.size }
					},
					limit: ctx.params.limit,
					sort: "-free"
				});

				return disks;
			}
		},

		/**
		 * probe node storage for devices
		 * 
		 * @actions
		 * @param {String} node - node id
		 * 
		 * @returns {Object} storage devices
		 */
		probe: {
			rest: {
				method: "GET",
				path: "/probe",
			},
			params: {
				node: { type: "string", empty: false, optional: false }
			},
			async handler(ctx) {
				// Find the node
				const node = await ctx.call('v1.nodes.resolve', { id: ctx.params.node });
				if (!node) {
					// Node not found
					throw new MoleculerClientError(
						`Node ${ctx.params.node} not found`,
						404, "NODE_NOT_FOUND", { node: ctx.params.node }
					);
				}


				// Probe node storage
				const disks = await this.probeNodeStorage(ctx, node);

				// Create an array of results
				const result = [];

				// Iterate over disks
				for (const disk of disks) {
					// Create query object
					const query = {
						name: disk.name,
						node: node.id
					};

					// If serial is not empty, add it to the query
					if (disk.serial) {
						query.serial = disk.serial
					}

					// Find if disk is already in the database
					const found = await this.findEntity(ctx, {
						query
					});

					// If not found, create it
					if (!found) {
						const created = await this.createEntity(ctx, {
							name: disk.name,
							size: disk.size,
							used: 0,
							rota: disk.rota,
							ro: disk.ro,
							model: disk.model,
							serial: disk.serial,
							mountpoint: disk.mountpoint,
							nfs: disk.nfs,
							cluster: this.config.get("storage.disks.defaultCluster"),
							node: node.id
						});

						// Add it to the result array
						result.push(created);
					} else {
						// If found, add it to the result array
						result.push(found);
					}
				}

				// Return the result array
				return result;
			},
		},


		/**
		 * Probe all nodes
		 * 
		 * @actions
		 * 
		 * @returns {Array} storage devices
		 */
		probeAll: {
			rest: {
				method: "GET",
				path: "/probe-all",
			},
			async handler(ctx) {
				const nodes = await ctx.call('v1.nodes.find', {});
				const result = [];

				for (const node of nodes) {
					const res = await this.actions.probe({ node: node.id }, { parentCtx: ctx })
						.catch(err => {
							console.log(err);
							return err
						})
					result.push(res);
				}

				return result;
			},
		},
		/**
		 * update disk used size
		 * 
		 * @actions
		 * @param {String} id - disk id
		 * 
		 * @returns {Object} updated disk
		 */
		updateUsed: {
			rest: {
				method: "PUT",
				path: "/:id/used",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}

				return this.updateUsed(ctx, disk);
			}
		},

		/**
		 * cron job to update disk used size
		 * 
		 * @actions
		 * 
		 * @returns {Array} updated disks
		 */
		updateAllUsed: {
			rest: {
				method: "GET",
				path: "/update-used",
			},
			async handler(ctx) {
				const disks = await this.findEntities(ctx, {});

				return Promise.all(disks.map(disk => {
					return this.actions.updateUsed({ id: disk.id }, { parentCtx: ctx });
				}));
			},
		},

		mountPoint: {
			rest: {
				method: "GET",
				path: "/:id/mount-point",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}
				if (disk.mountpoint == '/') {
					return '/mnt';
				}
				return disk.mountpoint;
			}
		},

		setNFS: {
			rest: {
				method: "PUT",
				path: "/:id/nfs",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
				nfs: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}
				return this.updateEntity(ctx, {
					id: disk.id,
					nfs: ctx.params.nfs
				});
			}
		},

		createFolder: {
			rest: {
				method: "POST",
				path: "/:id/folder",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
				path: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}
				return this.createFolder(ctx, disk, ctx.params.path);
			}
		},

		removeFolder: {
			rest: {
				method: "DELETE",
				path: "/:id/folder",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
				path: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}
				return this.removeFolder(ctx, disk, ctx.params.path);
			}
		},

		getFolderSize: {
			rest: {
				method: "GET",
				path: "/:id/folder-size",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
				path: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}
				return this.getFolderSize(ctx, disk, ctx.params.path);
			}
		},



		/**
		 * resolve nfs server for folder on disk
		 * 
		 * @actions
		 * @param {String} id - disk id
		 * @param {String} path - path on disk
		 * 
		 * @returns {Object} nfs server
		 */
		nfs: {
			rest: {
				method: "GET",
				path: "/:id/nfs",
			},
			params: {
				id: { type: "string", empty: false, optional: false },
				path: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const disk = await this.resolveStorageDevice(ctx, ctx.params.id);
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} not found`,
						404, "DISK_NOT_FOUND", { disk: ctx.params.id }
					);
				}

				if (!disk.nfs) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.id} has no NFS server`,
						404, "DISK_NO_NFS", { disk: ctx.params.id }
					);
				}

				const nfs = await ctx.call("v1.storage.nfs.resolve", { id: disk.nfs });
				if (!nfs) {
					throw new MoleculerClientError(
						`NFS server ${disk.nfs} not found`,
						404, "NFS_NOT_FOUND", { nfs: disk.nfs }
					);
				}

				return {
					server: nfs.ip,
					path: ctx.params.path
				}
			}
		},

		getNodeDisks: {
			rest: {
				method: "GET",
				path: "/node/:node",
			},
			params: {
				node: { type: "string", empty: false, optional: false },
			},
			async handler(ctx) {
				const node = await ctx.call("v1.nodes.resolve", { id: ctx.params.node });
				if (!node) {
					throw new MoleculerClientError(
						`Node ${ctx.params.node} not found`,
						404, "NODE_NOT_FOUND", { node: ctx.params.node }
					);
				}
				return this.findEntities(ctx, { query: { node: node.id } });
			}
		},

		clearDB: {
			rest: {
				method: "DELETE",
				path: "/clear",
			},
			async handler(ctx) {
				const disks = await this.findEntities(ctx, {});

				return Promise.all(disks.map(disk => {
					return this.removeEntity(ctx, { id: disk.id });
				}));
			}
		},
	},

	events: {

	},

	methods: {
		/**
		 * Resolve storage device
		 * 
		 * @param {Object} ctx
		 * @param {String} id
		 * 
		 * @returns {Object} disk
		 */
		async resolveStorageDevice(ctx, id) {
			return this.resolveEntities(ctx, { id });
		},
		/**
		 * Find disks by serial
		 * 
		 * @param {Object} ctx
		 * @param {Object} query
		 * 
		 * @returns {Array} disks
		 */
		async findBySerial(ctx, query) {
			return this.findEntity(ctx, { query });
		},

		/**
		 * Create folder
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * @param {String} folder
		 * 
		 * @returns {String} folder
		 */
		async createFolder(ctx, disk, folder) {
			const mountPoint = await this.actions.mountPoint({ id: disk.id }, { parentCtx: ctx });
			const path = `${mountPoint}${folder.startsWith('/') ? folder : `/${folder}`}`;
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `sudo mkdir -p ${path}`
			}).then(res => console.log(res));
			return `${path}`;
		},

		/**
		 * Remove folder
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * @param {String} folder
		 * 
		 * @returns {String} folder
		 */
		async removeFolder(ctx, disk, folder) {
			const mountPoint = await this.actions.mountPoint({ id: disk.id }, { parentCtx: ctx });
			const path = `${mountPoint}${folder.startsWith('/') ? folder : `/${folder}`}`;
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `sudo rm -rf ${path}`
			});
			return `${path}`;
		},

		/**
		 * Get folder size in MB
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * @param {String} folder
		 * 
		 * @returns {Number} size
		 */
		async getFolderSize(ctx, disk, folder) {
			const normilizedPath = folder;//path.normalize(folder);
			const mountPoint = await this.actions.mountPoint({ id: disk.id }, { parentCtx: ctx });
			const size = await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `du -sm ${mountPoint}${normilizedPath.startsWith('/') ? normilizedPath : `/${normilizedPath}`}`
			});
			console.log(size)
			return parseInt(size);
		},

		/**
		 * Probe node storage
		 * 
		 * @param {Object} ctx
		 * @param {Object} node
		 * 
		 * @returns {Array} disks
		 */
		async probeNodeStorage(ctx, node) {
			const lsblk = await ctx.call("v1.terminal.exec", {
				node: node.id,
				command: "lsblk -J -o NAME,SIZE,ROTA,RO,MODEL,SERIAL,MOUNTPOINT,TYPE",
			}).then(res => JSON.parse(res));

			const disks = [];
			lsblk.blockdevices.forEach(device => {
				this.parseDevice(device, disks);
			});
			return disks;
		},


		/**
		 * Convert size string to MB
		 * 
		 * @param {String} size
		 * 
		 * @returns {Number} size in MB
		 */
		sizeToMB(size) {
			const unit = size.slice(-1);
			const value = parseInt(size.slice(0, -1));

			if (unit === 'M') {
				return value;
			} else if (unit === 'G') {
				return value * 1024;
			} else if (unit === 'T') {
				return value * 1024 * 1024;
			} else if (unit === 'B') {
				return value;
			}

			throw new Error(`Unknown unit: ${unit}`);
		},

		/**
		 * Process children
		 * 
		 * @param {Array} children
		 * @param {Array} mountpoints
		 */
		processChildren(children, mountpoints) {
			children.forEach(child => {
				if (
					child.mountpoints &&
					child.mountpoints[0] &&
					child.mountpoints[0].startsWith('/') &&
					!child.mountpoints[0].includes('boot') &&
					!child.mountpoint.includes('SWAP')) {
					mountpoints.push({
						name: child.name,
						type: child.type,
						mountpoint: child.mountpoints[0],
						size: this.sizeToMB(child.size),
					});
				}

				if (child.children) {
					this.processChildren(child.children, mountpoints);
				}
			});
		},

		/**
		 * Parse device
		 * 
		 * This function takes a device object from the lsblk output and converts it into a format that
		 * is more suitable for our needs.
		 * 
		 * @param {Object} device
		 * 
		 * @returns {Object} parsed device
		 */
		parseDevice(device, disks) {
			// If the device is a disk
			if (device.type === "disk") {
				// Skip certain devices
				if (device.name.includes('boot') || device.model == 'MassStorageClass' || device.model == 'VIRTUAL-DISK') {
					return;
				}

				// Create a new device object
				const dev = {
					// The name of the device
					name: device.name,
					// The size of the device in MB
					size: this.sizeToMB(device.size),
					// Whether the device is a rotational disk
					rota: device.rota,
					// Whether the device is read only
					ro: device.ro,
					// The model name of the device
					model: device.model?.trim(),
					// The serial number of the device
					serial: device.serial?.trim(),
					// The mount point of the device
					mountpoint: null,
					// The mount points of the device's children
					mountpoints: []
				};

				// If the device has children, process them
				if (device.children) {
					// Process the children
					this.processChildren(device.children, dev.mountpoints);
				}

				// If the device has a mount point, set it
				if (device.mountpoints && device.mountpoints[0] && device.mountpoints[0].startsWith('/')) {
					dev.mountpoint = device.mountpoints[0];
				}

				// If the device doesn't have a mount point, but its children do, set the mount point
				// to the largest child's mount point
				if (!dev.mountpoint && device.children) {
					const rootPart = device.children
						.filter(child => child.mountpoint &&
							!child.mountpoint.includes('boot') &&
							!child.mountpoint.includes('SWAP'))
						.sort((a, b) => a.size - b.size)
						.pop();
					if (rootPart) {
						dev.mountpoint = rootPart.mountpoint;
					}
				}

				// If the device is mounted at /k3os/system, set the mount point to /
				if (dev.mountpoint == '/k3os/system') {
					dev.mountpoint = '/';
				}

				// Add the device to the list of disks
				disks.push(dev);
			}
		},


		/**
		 * Update disk used size
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * 
		 * @returns {Object} updated disk
		 */
		async updateUsed(ctx, disk) {
			const used = await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `df -P ${disk.mountpoint}`,
			}).then(res => {
				const lines = res.split('\n');
				const line = lines[1];
				const used = line.split(/\s+/)[2];
				return Number((Number(used) / 1024).toFixed(2));
			});
			if (isNaN(used)) return;
			return this.updateEntity(ctx, {
				id: disk.id,
				used,
				free: disk.size - used
			});
		},

		/**
		 * Format disk
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * 
		 * @returns {Object} updated disk
		 */
		async format(ctx, disk) {
			if (disk.formatted) {
				throw new MoleculerClientError(
					`Disk ${disk.name} is formatted`,
					409, "DISK_FORMATTED", { id: disk.id }
				);
			} else if (disk.mounted) {
				throw new MoleculerClientError(
					`Disk ${disk.name} is mounted`,
					409, "DISK_MOUNTED", { id: disk.id }
				);
			}
			const device = `/dev/${disk.name}`;
			const fsType = this.config.get("storage.disks.fsType");
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `mkfs -t ${fsType} ${device}`,
			});
			return this.updateEntity(ctx, {
				id: disk.id,
				mountpoint: `/mnt/${disk.name}`,
				fsType,
				formatted: true
			});
		},

		/**
		 * Mount disk
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * 
		 * @returns {Object} updated disk
		 */
		async mount(ctx, disk) {
			if (disk.mounted) {
				throw new MoleculerClientError(
					`Disk ${disk.name} is mounted`,
					409, "DISK_MOUNTED", { id: disk.id }
				);
			} else if (!disk.formatted) {
				throw new MoleculerClientError(
					`Disk ${disk.name} is not formatted`,
					409, "DISK_NOT_FORMATTED", { id: disk.id }
				);
			}
			const device = `/dev/${disk.name}`;
			const mountpoint = disk.mountpoint;
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `mkdir -p ${mountpoint}`,
			});
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `mount ${device} ${mountpoint}`,
			});
			return this.updateEntity(ctx, {
				id: disk.id,
				mounted: true
			});
		},


		/**
		 * Unmount disk
		 * 
		 * @param {Object} ctx
		 * @param {Object} disk
		 * 
		 * @returns {Object} updated disk
		 */
		async unmount(ctx, disk) {
			if (!disk.mounted) {
				throw new MoleculerClientError(
					`Disk ${disk.name} is not mounted`,
					409, "DISK_NOT_MOUNTED", { id: disk.id }
				);
			}
			const mountpoint = disk.mountpoint;
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `umount ${mountpoint}`,
			});
			await ctx.call("v1.terminal.exec", {
				node: disk.node,
				command: `rm -rf ${mountpoint}`,
			});
			return this.updateEntity(ctx, {
				id: disk.id,
				mounted: false
			});
		},

	},

	created() { },

	async started() { },

	async stopped() { }
};