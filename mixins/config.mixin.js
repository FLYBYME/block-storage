
"use strict";
const { MoleculerClientError } = require("moleculer").Errors;
const Context = require("moleculer").Context;

module.exports = {
    name: "config",
    version: 1,

    dependencies: [{
        name: "config",
        version: 1,
    }],


    created() {
        this.config = new Map();
    },
    async started() {
        "use strict";
        const { MoleculerClientError } = require("moleculer").Errors;
        const Context = require("moleculer").Context;
        
        module.exports = {
            name: "config",
            version: 1,
        
            dependencies: [{
                name: "config",
                version: 1,
            }],
        
        
            created() {
                this.config = new Map();
            },
            async started() {
                const keys = Object.keys(this.settings.config || {});
                for (const key of keys) {
                    const value = this.settings.config[key];
                    const found = await this.broker.call("v1.config.get", { key });
                    if (!found) {
                        await this.broker.call("v1.config.set", { key, value })
                            .catch(err => {
                                this.logger.error(`Config error: ${err.message}`, key, value);
                            });
                    }
                }
                const all = await this.broker.call("v1.config.all");
                for (const key in all) {
                    this.config.set(key, all[key]);
                }
        
                this.logger.info('Config loaded');
            }
        };
        const keys = Object.keys(this.settings.config || {});
        for (const key of keys) {
            const value = this.settings.config[key];
            const found = this.config.get(key);
            if (!found) {
                this.config.set(key, value);
            }
        }
        const all = await this.broker.call("v1.config.all");
        for (const key in all) {
            this.config.set(key, all[key]);
        }

        this.logger.info('Config loaded');
    }
};