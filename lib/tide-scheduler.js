var fs = require("fs");
var async = require("async");
var _ = require("lodash");
var Job = require([__dirname, "job"].join("/"));

const JOBS_KEY = "tideJobs";
const TIMEOUT = 1000;

module.exports = {

    initialize: function(core){
        var self = this;
        this.core = core;
        this.myraid_key =  [core.constants.myriad.CLUSTER_ID, JOBS_KEY].join(core.constants.myriad.DELIMITER);
        this.jobs_locked = false;
        this.defrost_jobs();
    },

    jobs: {},

    add_job: function(config, fn){
        var self = this;

        const potentialJobs = _.clone(this.jobs);
        potentialJobs[config.id] = new Job(config);

        this.persist_jobs(potentialJobs, (err) => {
            if(!err) {
                this.core.loggers["tide-scheduler"].log("verbose", ["Added job:", config.id].join(" "));
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

                    this.core.applications.add(config.application, function(err){
                        self.jobs[config.id].schedule(function(){
                            self.core.loggers["tide-scheduler"].log("verbose", ["Created tide application", config.application.id].join(" "));

                            self.core.applications.remove(config.application.id, function(err){
                                self.core.applications.add(config.application, function(err){
                                    async.timesSeries(config.instances, function(index, fn){
                                        self.core.applications.deploy_container(config.application.id, {}, fn);
                                    }, function(){
                                        var interval = setInterval(function(){
                                            self.core.cluster.myriad.persistence.keys([self.core.constants.myriad.CONTAINERS_PREFIX, config.application.id, "*"].join(self.core.constants.myriad.DELIMITER), function(err, containers){
                                                if(!err && containers.length == 0)
                                                    clearInterval(interval);
                                            });
                                        }, 15000);
                                    });
                                });
                            });
                        });
                    });

                    return fn();
                }
            } else {
                return fn(err);
            }
        });
    },

    update_job: function(id, new_config, fn) {
        var self = this;

        var existing_config = this.jobs[id].config;
        var config = _.merge(existing_config, new_config);
        this.remove_job(id, function(err){
            if(err && err.key != "ENOKEY"){
                return fn({
                    code: 400
                });
            }

            self.add_job(config, fn);
        });
    },

    remove_job: function(id, fn){
        if(this.core.cluster.praetor.is_controlling_leader()) {
            const jobToRemove = this.jobs[id];
            const potentialJobs = _.omit(jobs, [id]);
            this.persistJobs(potentialJobs, (err) => {
                this.core.loggers["tide-scheduler"].log("verbose", ["Removed job:", id].join(" "));
                jobToRemove.cancel();
            });
        }

        this.core.applications.remove(id, fn);
    }

    defrost_jobs: function(fn) {
        if (!this.jobs_locked) {
            this.jobs_locked = true;
            this.core.cluster.myriad.persistence.get(this.myriad_key, (err, jobs) => {
                this.jobs_locked = false;
                if(err) {
                    this.core.loggers["tide-scheduler"].log("error", _.join(["Error defrosting jobs:", err], " "));
                    this.jobs = {};
                    if(fn) fn(err);
                } else {
                    this.jobs = _.mapObject(jobs, (j) => new Job(j));
                    if(fn) fn();
                }
            });
        } else {
            setTimeout(() => { this.defrostJobs(fn); }, TIMEOUT);
        }
    }

    persist_jobs: function(jobs, fn) {
        if(!this.jobs_locked) {
            this.jobs_locked = true;
            const serializedJobs = _.mapObject(jobs, (j) => JSON.stringify(j.serialize()));
            this.core.cluster.myriad.persistence.set(this.myriad_key, serializedJobs, (err) => {
                this.jobs_locked = false;
                if(err) {
                    this.core.loggers["tide-scheduler"].log("error", _.join(["Error persisting jobs:", err], " "));
                    if(fn) fn(err);
                } else {
                    this.jobs = jobs;
                    if(fn) fn();
                }
            });
        } else {
            setTimeout(() => { this.persistJobs(jobs, fn); }, TIMEOUT);
        }
    }
}
        
