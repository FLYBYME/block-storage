"use strict";
const DbService = require("@moleculer/database").Service;
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;

const ConfigMixin = require("../mixins/config.mixin");


module.exports = {
	name: "zones",
	version: 1,

	mixins: [
		DbService({
			createActions: false,
			adapter: {
				type: "NeDB",
				options: "./db/zones.db"
			}
		}),
		ConfigMixin
	],

	settings: {
		fields: {
			// zone name
			name: { type: 'string', empty: false, required: true },


			// zone description
			description: { type: 'string', empty: false, required: false },

			// zone status
			status: {
				type: 'enum',
				values: [
					'active', 'inactive', 'online', 'offline', 'pending', 'failed', 'degraded'
				],
				default: 'pending',
				required: true
			},

			// zone location
			location: {
				type: 'object',
				properties: {
					country: { type: 'string', required: true },
					region: { type: 'string', required: true },
				}
			},

			// zone provider
			provider: {
				type: 'string',
				required: false,
				empty: false,
				default: 'ovh',
				enum: ['ovh', 'tna', 'aws', 'gcp', 'azure']
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
	},

	actions: {
		/**
		 * lookup zone
		 * 
		 * @actions
		 * @param {String} name - zone name
		 * 
		 * @returns {Object} - zone object
		 */
		lookup: {
			rest: {
				method: "GET",
				path: "/lookup",
			},
			params: {
				name: { type: "string", empty: false, optional: false }
			},
			async handler(ctx) {
				return this.findEntity(ctx, { query: { name: ctx.params.name } });
			}
		},

		/**
		 * Seed DB
		 *
		 * @actions
		 */
		seedDB: {
			rest: {
				method: "GET",
				path: "/seed",
			},
			async handler(ctx) {
				return await this.seedDB();
			}
		}
	},

	events: {

	},

	methods: {
		/**
				 * seed DB
				 */
		async seedDB() {
			this.logger.info("Seeding DB")
			const zones = [
				{
					name: 'ca',
					description: 'Canadian zone provided by OVH',
					status: 'active',
					location: {
						country: 'Canada',
						region: 'Beauharnois'
					},
					provider: 'ovh'
				},
				{
					name: 'usa',
					description: 'Chicago zone provided by TNA',
					status: 'active',
					location: {
						country: 'USA',
						region: 'Chicago'
					},
					provider: 'tna'
				},
				{
					name: 'syd',
					description: 'Sydney zone provided by OVH',
					status: 'active',
					location: {
						country: 'Australia',
						region: 'Sydney'
					},
					provider: 'ovh'
				},
				{
					name: 'uk',
					description: 'London zone provided by OVH',
					status: 'active',
					location: {
						country: 'UK',
						region: 'London'
					},
					provider: 'ovh'
				}
			]
			const ctx = new Context(this.broker)
			for (const zone of zones) {
				await this.createEntity(ctx, zone);
			}
		}
	},

	created() { },

	async started() { },

	async stopped() { }
};