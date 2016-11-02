var fs = require("fs");
var async = require("async");
var _ = require("lodash");
var Job = require([__dirname, "job"].join("/"));

module.exports = {

    initialize: function(core) {
        this.core = core;
        this.myriad_key = [this.core.constants.myriad.VARIABLES_PREFIX, "tide-jobs"].join(this.core.constants.myriad.DELIMITER);

        const on_defrost = (err) => {
            if(err) {
                this.core.loggers["tide-scheduler"].log("verbose", ["Unable to restore tide jobs from myraid-kv:", JSON.stringify(err)].join(" "));
                throw err;
            } else {
                this.core.loggers["tide-scheduler"].log("verbose", ["Successfully restored", _.size(this.jobs), "tide applications from myriad-kv."].join(" "));
            }

        };

        if(this.core.cluster.praetor.is_controlling_leader()) {
            this.defrostJobs(on_defrost);
        }

        this.core.cluster.legiond.on("myriad.bootstrapped", () => {
            this.defrostJobs(on_defrost);
        });
    },

    jobs: {},

    schedule_job: function(config) {
        if(_.has(this.jobs, config.id)) {
            this.jobs[config.id].schedule(() => {
                this.core.loggers["tide-scheduler"].log("verbose", ["Deplyoing tide application", config.application.id, "to", config.instances, "containers."].join(" "));

                this.core.applications.remove(config.application.id, (err) => {
                    this.core.applications.add(config.application, (err) => {
                        async.timesSeries(config.instances, (index, fn) => {
                            this.core.applications.deploy_container(config.application.id, {}, fn);
                        }, () => {
                            var interval = setInterval(() => {
                                this.core.cluster.myriad.persistence.keys([this.core.constants.myriad.CONTAINERS_PREFIX, config.application.id, "*"].join(this.core.constants.myriad.DELIMITER), (err, containers) => {
                                    if(!err && containers.length == 0)
                                        clearInterval(interval);
                                });
                            }, 15000);
                        });
                    });
                });
            });
        }
    },

    add_job: function(config, fn){
        if(this.core.cluster.praetor.is_controlling_leader()){
            config.application.id = config.id;

            if(!_.has(config.application, "tags"))
                config.application.tags = {};

            if(!_.has(config.application.tags, "metadata"))
                config.application.tags.metadata = {};

            config.application.tags.metadata.ancestry = "containership.plugin";
            config.application.tags.metadata.plugin = "tide";

            // force no respawn on application containers
            config.application.respawn = false;

            this.jobs[config.id] = new Job(config);

            this.persistJobs((err) => {
                if(err) {
                    this.core.loggers["tide-scheduler"].log("error", ["Error adding job:", config.id].join(" "));
                    delete this.jobs[config.id];
                    return fn(err);
                } else {
                    this.core.loggers["tide-scheduler"].log("verbose", ["Added job:", config.id].join(" "));

                    this.core.applications.add(config.application, (err) => {
                        this.schedule_job(config);
                    });

                    return fn();
                }
            });
        }
    },

    update_job: function(id, new_config, fn){
        var existing_config = this.jobs[id].config;
        var config = _.merge(existing_config, new_config);
        this.remove_job(id, (err) => {
            if(err && err.key != "ENOKEY"){
                return fn({
                    code: 400
                });
            }

            this.add_job(config, fn);
        });
    },

    remove_job: function(id, fn){
        if(this.core.cluster.praetor.is_controlling_leader()) {
            const removedJob = this.jobs[id];
            delete this.jobs[id];

            this.persistJobs((err) => {
                if(err) { 
                    this.core.loggers["tide-scheduler"].log("verbose", ["Failed removing job:", id].join(" "));
                    this.jobs[id] = removedJob;
                    return fn(err);
                } else {
                    this.core.loggers["tide-scheduler"].log("verbose", ["Removed job:", id].join(" "));
                    removedJob.cancel();
                    return this.core.applications.remove(id, fn);
                }
            });
        }

    },

    persistJobs: function(fn) {
        this.core.cluster.myriad.persistence.set(this.myriad_key, _.map(this.jobs, (j) => { 
            return j.serialize(); 
        }), fn);
    },

    defrostJobs: function(fn) {
        this.core.cluster.myriad.persistence.get(this.myriad_key, (err, jobs) => {
            console.log("Results:");
            console.log("Err: " + err + " " + jobs);
            if(err) {
                return fn(err);
            } else {
                try {
                    jobs = JSON.parse(jobs);
                } catch(err) {
                    return fn(err);
                }

                const old_jobs = this.jobs;

                this.jobs = _.map(jobs, (j) => {
                    return new Job(j);
                });

                _.forEach(this.jobs, (j) => {
                    if(_.has(old_jobs, j.config.id)) {
                        this.core.applications.remove(j.config.id, () => this.schedule_job(j.config));
                    } else {
                        this.schedule_job(j.config);
                    }
                });

                if(!_.isEmpty(oldJobs)) {
                    _.forEach(oldJobs, (j) => {
                        j.cancel();
                    });

                    _.forEach(_.omit(oldJobs, _.keys(this.jobs)), (j) => {
                        this.core.applications.remove(j.config.id);
                    });
                }

                return fn();
            }
        });
    }

}
