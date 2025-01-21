"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;

const ConfigMixin = require("../mixins/config.mixin");

const Moniker = require("moniker");


module.exports = {
	name: "storage.nfs",
	version: 1,

	mixins: [
		DbService({
			createActions: true,
			adapter: {
				type: "NeDB",
				options: "./db/storage.nfs.db"
			}
		}),
		ConfigMixin
	],

	settings: {
		fields: {

			// nfs name
			name: {
				type: "string",
				required: true,
				unique: true
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

			// disk id
			disk: {
				type: "string",
				required: true,
				empty: false,
				populate: {
					action: "v1.storage.disks.resolve",
				}
			},

			// cluster id
			cluster: {
				type: "string",
				required: true,
				empty: false
			},

			// namespace
			namespace: {
				type: "string",
				required: true,
				empty: false
			},

			// pod
			pod: {
				type: "string",
				required: false,
				empty: false
			},

			// fqdn
			fqdn: {
				type: "string",
				required: true
			},

			// ip
			ip: {
				type: "string",
				required: false
			},

			// path
			path: {
				type: "string",
				required: true
			},

			// pod id
			uid: {
				type: "string",
				required: false,
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
			"storage.defaultCluster": "test",
			"storage.nfs.domain": "one-host.ca",
			"storage.nfs.namespace": "storage",
		}
	},

	actions: {

		/**
		 * create a new nfs server
		 * 
		 * @actions
		 * @param {String} disk - disk id
		 * 
		 * @returns {Object} nfs object
		 */
		createNFS: {
			rest: {
				method: 'POST',
				path: '/create-nfs',
			},
			params: {
				disk: { type: 'string', empty: false, required: true },
			},
			async handler(ctx) {
				const disk = await ctx.call("v1.storage.disks.resolve", {
					id: ctx.params.disk
				});
				if (!disk) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.disk} not found`,
						404, "DISK_NOT_FOUND", { id: ctx.params.disk }
					);
				}

				const node = await ctx.call("v1.nodes.resolve", {
					id: disk.node,
					//fields: ['cluster']
				});
				if (!node) {
					throw new MoleculerClientError(
						`Node ${ctx.params.node} not found`,
						404, "NODE_NOT_FOUND", { id: ctx.params.node }
					);
				}

				// check if disk already has nfs server
				if (disk.nfs) {
					throw new MoleculerClientError(
						`Disk ${ctx.params.disk} already has nfs server`,
						400, "DISK_HAS_NFS", { id: ctx.params.disk }
					);
				}

				return this.createNFS(ctx, node, disk);
			}
		},


		/**
		 * Create NFS kubernetes pod for nfs server
		 * 
		 * @actions
		 * @param {String} disk - disk id
		 * 
		 * @returns {Object} nfs object
		 */
		createPod: {
			rest: {
				method: 'POST',
				path: '/:id/create-pod',
			},
			params: {
				id: { type: 'string', empty: false, required: true },
			},
			async handler(ctx) {
				const nfs = await this.resolveNFS(ctx, ctx.params.id);
				if (!nfs) {
					throw new MoleculerClientError(
						`NFS server ${ctx.params.id} not found`,
						404, "NFS_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.createNFSPod(ctx, nfs);
			}
		},


		/**
		 * Delete NFS kubernetes pod for nfs server
		 * 
		 * @actions
		 * @param {String} disk - disk id
		 * 
		 * @returns {Object} nfs object
		 */
		deletePod: {
			rest: {
				method: 'POST',
				path: '/:id/delete-pod',
			},
			params: {
				id: { type: 'string', empty: false, required: true },
			},
			async handler(ctx) {
				const nfs = await this.resolveNFS(ctx, ctx.params.id);
				if (!nfs) {
					throw new MoleculerClientError(
						`NFS server ${ctx.params.id} not found`,
						404, "NFS_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return this.deleteNFSPod(ctx, nfs);
			}
		},


		/**
		 * Get NFS kubernetes pod for nfs server
		 * 
		 * @actions
		 * @param {String} disk - disk id
		 * 
		 * @returns {Object} nfs object
		 */
		getPod: {
			rest: {
				method: 'GET',
				path: '/:id/get-pod',
			},
			params: {
				id: { type: 'string', empty: false, required: true },
			},
			async handler(ctx) {
				const nfs = await this.resolveNFS(ctx, ctx.params.id);
				if (!nfs) {
					throw new MoleculerClientError(
						`NFS server ${ctx.params.id} not found`,
						404, "NFS_NOT_FOUND", { id: ctx.params.id }
					);
				}

				return ctx.call("v1.kubernetes.readNamespacedPod", {
					cluster: nfs.cluster,
					namespace: nfs.namespace,
					name: nfs.name
				});
			}
		}
	},

	events: {
		async "kubernetes.pods.deleted"(ctx) {
			const pod = ctx.params;
			const nfs = await this.resolveNFSPod(pod.ctx, pod.metadata.uid);
			if (!nfs) {
				return;
			}

			console.log(nfs, pod)
		},
		async "kubernetes.pods.added"(ctx) {
			const pod = ctx.params;
			const nfs = await this.resolveNFSPod(pod.ctx, pod.metadata.uid);
			if (!nfs) {
				return;
			}
			console.log(nfs, pod)
		},
		async "kubernetes.pods.modified"(ctx) {
			const pod = ctx.params;
			const nfs = await this.resolveNFSPod(pod.ctx, pod.metadata.uid);
			if (!nfs) {
				return;
			}
			if (pod.status.phase == "Running" && !nfs.ip) {
				await this.updateEntity(pod.ctx, {
					id: nfs.id,
					ip: pod.status.podIP
				});

				this.logger.info(`NFS pod ${pod.id} updated with IP ${pod.status.podIP}`);
			} else {
				console.log(nfs, pod)
			}
		}
	},

	methods: {
		async resolveNFS(ctx, id) {
			return this.resolveEntities(ctx, { id });
		},
		async resolveNFSPod(ctx, uid) {
			return this.findEntity(ctx, { query: { pod: uid } });
		},

		/**
		 * Create a new NFS server on node
		 * 
		 * @param {Object} ctx - moleculer context
		 * @param {Object} node - node object
		 * @param {Object} disk - disk object
		 * 
		 * @returns {Object} nfs object
		 */
		async createNFS(ctx, node, disk) {
			const name = Moniker.choose();

			const fqdn = `${name}.nfs.storage.${this.config.get("storage.nfs.domain")}`;
			const path = await ctx.call("v1.storage.disks.mountPoint", { id: disk.id });

			const namespace = this.config.get("storage.nfs.namespace");

			// create a new nfs server
			const nfs = await this.createEntity(ctx, {
				name: name,
				node: node.id,
				disk: disk.id,
				cluster: this.config.get("storage.defaultCluster"),
				namespace: namespace,
				fqdn: fqdn,
				path: path
			});

			// update disk with nfs server
			await ctx.call("v1.storage.disks.setNFS", {
				id: disk.id,
				nfs: nfs.id
			});

			this.logger.info(`NFS server ${nfs.id} created`);

			return nfs;
		},

		/**
		 * Create NFS kubernetes pod for nfs server
		 * 
		 * @param {Object} ctx - moleculer context
		 * @param {Object} nfs - nfs object
		 * 
		 * @returns {Object} nfs object
		 */
		async createNFSPod(ctx, nfs) {
			this.logger.info(`Creating NFS pod for nfs server ${nfs.id}`);

			// check if nfs server already has a pod
			const found = await ctx.call("v1.kubernetes.findOne", {
				cluster: nfs.cluster,
				namespace: nfs.namespace,
				name: nfs.name
			});
			if (found) {
				this.logger.info(`NFS pod ${found.id} already exists`);
				return found;
			}

			const node = await ctx.call("v1.nodes.resolve", {
				id: nfs.node
			});
			if (!node) {
				throw new MoleculerClientError(
					`Node ${nfs.node} not found`,
					404, "NODE_NOT_FOUND", { id: nfs.node }
				);
			}

			const metadata = {
				name: nfs.name,
				namespace: nfs.namespace
			};

			const containers = [{
				name: nfs.name,
				image: 'itsthenetwork/nfs-server-alpine:latest',
				imagePullPolicy: 'IfNotPresent',
				ports: [{
					containerPort: 2049,
					protocol: 'TCP',
					name: 'nfs'
				}],
				env: [{
					name: 'SHARED_DIRECTORY',
					value: '/nfsshare'
				}],
				volumeMounts: [{
					name: 'nfs',
					mountPath: '/nfsshare'
				}],
				resources: {
					limits: {
						cpu: '100m',
						memory: '100Mi'
					},
					requests: {
						cpu: '10m',
						memory: '10Mi'
					}
				},
				securityContext: {
					privileged: true
				}
			}];

			// host path for nfs server
			const nfsPath = await ctx.call("v1.storage.disks.mountPoint", {
				id: nfs.disk
			});
			const volumes = [{
				name: 'nfs',
				hostPath: {
					path: nfsPath
				}
			}];

			// node nodeName 

			const pod = await ctx.call("v1.kubernetes.createNamespacedPod", {
				cluster: nfs.cluster,
				namespace: nfs.namespace,
				body: {
					apiVersion: "v1",
					kind: "Pod",
					metadata: metadata,
					spec: {
						containers,
						volumes,
						nodeName: node.hostname
					},
				}
			});

			// update nfs server
			const updated = await this.updateEntity(ctx, {
				id: nfs.id,
				pod: pod.metadata.uid,
				ip: pod.status.podIP
			})

			this.logger.info(`NFS pod ${updated.id} created`);

			return updated;
		},

		/**
		 * Delete NFS kubernetes pod for nfs server
		 * 
		 * @param {Object} ctx - moleculer context
		 * @param {Object} nfs - nfs object
		 * 
		 * @returns {Object} nfs object
		 */
		async deleteNFSPod(ctx, nfs) {
			this.logger.info(`Deleting NFS pod for nfs server ${nfs.id}`);

			// check if nfs server already has a pod
			const found = await ctx.call("v1.kubernetes.readNamespacedPod", {
				cluster: nfs.cluster,
				namespace: nfs.namespace,
				name: nfs.name
			});
			if (!found) {
				this.logger.info(`NFS pod ${found.id} not found`);
				return nfs;
			}

			await ctx.call("v1.kubernetes.deleteNamespacedPod", {
				cluster: nfs.cluster,
				namespace: nfs.namespace,
				name: nfs.name
			});

			// update nfs server
			const updated = await this.updateEntity(ctx, {
				id: nfs.id,
				pod: null,
				ip: null
			});

			this.logger.info(`NFS pod ${found.id} deleted`);

			return updated;
		},
	},

	created() { },

	async started() { },

	async stopped() { }
};